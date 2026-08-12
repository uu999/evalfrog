package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/adapters/cacheredis"
	"github.com/uu999/evalfrog/internal/adapters/httpapi"
	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/adapters/postgres"
	"github.com/uu999/evalfrog/internal/adapters/schedulingredis"
	"github.com/uu999/evalfrog/internal/adapters/workerapi"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/bootstrap"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/httpserver"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/lifecycle"
	"github.com/uu999/evalfrog/internal/platform/logging"
	"github.com/uu999/evalfrog/internal/platform/metrics"
	"github.com/uu999/evalfrog/internal/platform/migrations"
	"github.com/uu999/evalfrog/internal/recovery"
	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/runtime/engine"
	"github.com/uu999/evalfrog/internal/scheduling"
)

const serviceName = "evalfrog-control-plane"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, arguments []string, output, errorOutput io.Writer) int {
	options, err := bootstrap.Parse(serviceName, arguments, errorOutput)
	if err != nil {
		return 2
	}
	if options.HealthcheckURL != "" {
		if err := bootstrap.Probe(ctx, options.HealthcheckURL); err != nil {
			fmt.Fprintln(errorOutput, err)
			return 1
		}
		return 0
	}
	configuration, err := bootstrap.Load(options)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	if options.CheckConfig {
		fmt.Fprintf(output, "configuration valid: service=%s profile=%s\n", serviceName, configuration.Profile)
		return 0
	}
	logger, err := logging.New(output, serviceName, configuration.Observability.LogLevel)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	postgresClient, err := postgres.Open(ctx, configuration.Postgres)
	if err != nil {
		logger.Error("PostgreSQL client creation failed", "error", err)
		return 1
	}
	defer postgresClient.Close()
	if options.Migrate {
		runner := migrations.Runner{
			Pool: postgresClient.Pool(), Schema: configuration.Postgres.Schema,
			Directory: configuration.Migrations.Directory, LockTimeout: configuration.Migrations.LockTimeout.Duration(),
		}
		if err := runner.Up(ctx); err != nil {
			logger.Error("migration failed", "error", err)
			return 1
		}
		logger.Info("migrations applied", "schema", configuration.Postgres.Schema)
		return 0
	}

	schedulingClient := schedulingredis.Open(configuration.Redis.Scheduling)
	defer closeWithLog(logger, "scheduling Redis", schedulingClient.Close)
	cacheClient := cacheredis.Open(configuration.Redis.Cache)
	defer closeWithLog(logger, "cache Redis", cacheClient.Close)
	kafkaClient, err := kafka.Open(configuration.Kafka, serviceName)
	if err != nil {
		logger.Error("Kafka client creation failed", "error", err)
		return 1
	}
	defer kafkaClient.Close()

	readiness := health.New(configuration.Redis.Cache.OperationTimeout.Duration())
	mustRegister(logger, readiness, "postgres", postgresClient.Check)
	mustRegister(logger, readiness, "redis-cache", cacheClient.Check)
	mustRegister(logger, readiness, "redis-scheduling", schedulingClient.Check)
	mustRegister(logger, readiness, "kafka", kafkaClient.Check)
	server := httpserver.New(
		serviceName, configuration.HTTP.ControlPlaneAddress,
		configuration.HTTP.ReadHeaderTimeout.Duration(), configuration.HTTP.IdleTimeout.Duration(),
		logger, readiness, metrics.New(serviceName),
	)
	store := postgres.NewStore(postgresClient.Pool())
	accessService := access.NewService(store)
	resourceResolver := resources.NewResolver(store, accessService)
	definitionService := definition.NewBuiltinService(store, accessService, resourceResolver)
	server.Handle("/v1/", httpapi.New(accessService, definitionService))
	attemptCoordinator := attempt.NewBuiltinCoordinator(store)
	contextGateway, err := runtimecontext.NewGateway(store, cacheClient,
		configuration.Cache.ExecutionSnapshotTTL.Duration(), configuration.Cache.ActiveRunContextTTL.Duration())
	if err != nil {
		logger.Error("execution context gateway construction failed", "error", err)
		return 1
	}
	server.Handle("/internal/v1/", workerapi.NewHandler(attemptCoordinator, contextGateway, schedulingClient))
	schedulerID, err := (identity.UUIDv7Generator{}).New()
	if err != nil {
		logger.Error("scheduler identity generation failed", "error", err)
		return 1
	}
	projectScheduler, err := scheduling.New(store, schedulingClient,
		schedulingClient, identity.UUIDv7Generator{}, clock.System{}, schedulerID, scheduling.Settings{
			LaneCount: configuration.Scheduler.LaneCount, CreditGrantBatch: configuration.Scheduler.CreditGrantBatch,
			CandidateBatch: configuration.Scheduler.RedisCandidateBatch, AdmissionConcurrency: configuration.Scheduler.AdmissionConcurrency,
			Epoch: configuration.Scheduler.Epoch.Duration(), ActiveProjectTTL: configuration.Scheduler.ActiveProjectTTL.Duration(),
			BalancerLease: configuration.Scheduler.ActiveProjectTTL.Duration(), ReservationTTL: configuration.Scheduler.InflightReservationTTL.Duration(),
			DispatchBufferFactor: configuration.Scheduler.DispatchBufferFactor, CapacityChangeLimit: configuration.Scheduler.EpochCapacityChangeLimit,
		})
	if err != nil {
		logger.Error("scheduler construction failed", "error", err)
		return 1
	}
	schedulerService, err := scheduling.NewService(projectScheduler, "scheduler-"+schedulerID, logger)
	if err != nil {
		logger.Error("scheduler service construction failed", "error", err)
		return 1
	}
	runtimeRelay, err := eventing.NewRelay(store, kafkaClient, "runtime-relay-"+schedulerID,
		configuration.Outbox.Batch, configuration.Outbox.ClaimLease.Duration(), configuration.Outbox.ActivePoll.Duration())
	if err != nil {
		logger.Error("runtime relay construction failed", "error", err)
		return 1
	}
	runtimeRelayService, err := eventing.NewRelayService("runtime-outbox-relay", runtimeRelay,
		configuration.Outbox.ActivePoll.Duration(), configuration.Outbox.IdlePollMax.Duration(), logger)
	if err != nil {
		logger.Error("runtime relay service construction failed", "error", err)
		return 1
	}
	taskRelay, err := eventing.NewTaskRelay(store, kafkaClient, "task-relay-"+schedulerID,
		configuration.Outbox.Batch, configuration.Outbox.ClaimLease.Duration(), configuration.Outbox.ActivePoll.Duration())
	if err != nil {
		logger.Error("task relay construction failed", "error", err)
		return 1
	}
	taskRelayService, err := eventing.NewRelayService("task-outbox-relay", taskRelay,
		configuration.Outbox.ActivePoll.Duration(), configuration.Outbox.IdlePollMax.Duration(), logger)
	if err != nil {
		logger.Error("task relay service construction failed", "error", err)
		return 1
	}
	runtimeKafka, err := kafka.OpenConsumer(configuration.Kafka, serviceName+"-engine", "runtime-engine-v1",
		[]config.KafkaTopicConfig{configuration.Kafka.Topics.RuntimeEvent}, configuration.Kafka.RuntimeMaxPollRecords)
	if err != nil {
		logger.Error("runtime Kafka consumer creation failed", "error", err)
		return 1
	}
	defer runtimeKafka.Close()
	engineConsumer, err := engine.NewConsumer(store)
	if err != nil {
		logger.Error("engine consumer construction failed", "error", err)
		return 1
	}
	runtimeConsumerService, err := eventing.NewRuntimeConsumerService(runtimeKafka, engineConsumer, logger)
	if err != nil {
		logger.Error("runtime consumer service construction failed", "error", err)
		return 1
	}
	recoveryGrace := configuration.Worker.LostAfter.Duration() - configuration.Worker.LeaseDuration.Duration()
	reaper, err := recovery.NewReaper(store, attemptCoordinator, recoveryGrace,
		configuration.Worker.RecoveryScannerInterval.Duration(), configuration.Outbox.ScanBatch, "reaper-"+schedulerID, logger)
	if err != nil {
		logger.Error("attempt reaper construction failed", "error", err)
		return 1
	}
	if err := lifecycle.Run(ctx, configuration.Shutdown.Timeout.Duration(), logger, server, schedulerService,
		runtimeRelayService, taskRelayService, runtimeConsumerService, reaper); err != nil {
		logger.Error("control plane stopped with error", "error", err)
		return 1
	}
	return 0
}

func mustRegister(logger *slog.Logger, registry *health.Registry, name string, check health.Check) {
	if err := registry.Register(name, check); err != nil {
		logger.Error("health check registration failed", "check", name, "error", err)
		panic(err)
	}
}

func closeWithLog(logger *slog.Logger, name string, close func() error) {
	if err := close(); err != nil {
		logger.Warn("dependency close failed", "dependency", name, "error", err)
	}
}
