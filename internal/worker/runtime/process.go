package runtime

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/adapters/workerapi"
	"github.com/uu999/evalfrog/internal/platform/bootstrap"
	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/httpserver"
	"github.com/uu999/evalfrog/internal/platform/lifecycle"
	"github.com/uu999/evalfrog/internal/platform/logging"
	"github.com/uu999/evalfrog/internal/platform/metrics"
)

func RunProcess(ctx context.Context, arguments []string, resourceClass string, output, errorOutput io.Writer) int {
	service := "evalfrog-worker-" + resourceClass
	options, err := bootstrap.Parse(service, arguments, errorOutput)
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
	if options.Migrate {
		fmt.Fprintln(errorOutput, "workers cannot run PostgreSQL migrations")
		return 2
	}
	configuration, err := bootstrap.Load(options)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	if options.CheckConfig {
		fmt.Fprintf(output, "configuration valid: service=%s profile=%s\n", service, configuration.Profile)
		return 0
	}
	logger, err := logging.New(output, service, configuration.Observability.LogLevel)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	kafkaClient, err := kafka.Open(configuration.Kafka, service)
	if err != nil {
		logger.Error("Kafka client creation failed", "error", err)
		return 1
	}
	defer kafkaClient.Close()
	controlPlane := workerapi.New(configuration.Endpoints.ControlPlaneURL, configuration.Worker.ClaimTimeout.Duration())
	readiness := health.New(configuration.Redis.Cache.OperationTimeout.Duration())
	mustRegister(logger, readiness, "kafka", kafkaClient.Check)
	mustRegister(logger, readiness, "control-plane", controlPlane.Check)
	address := configuration.HTTP.BuiltinWorkerAddress
	if resourceClass == "sandbox" {
		address = configuration.HTTP.SandboxWorkerAddress
	}
	server := httpserver.New(
		service, address, configuration.HTTP.ReadHeaderTimeout.Duration(), configuration.HTTP.IdleTimeout.Duration(),
		logger, readiness, metrics.New(service),
	)
	shell := NewShell(resourceClass, logger)
	if err := lifecycle.Run(ctx, configuration.Shutdown.Timeout.Duration(), logger, server, shell); err != nil {
		logger.Error("worker stopped with error", "error", err)
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
