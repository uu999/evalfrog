package runtime

import (
	"context"
	"log/slog"
	"sync"
)

type Shell struct {
	resourceClass string
	logger        *slog.Logger
	stopped       chan struct{}
	once          sync.Once
}

func NewShell(resourceClass string, logger *slog.Logger) *Shell {
	return &Shell{resourceClass: resourceClass, logger: logger, stopped: make(chan struct{})}
}

func (shell *Shell) Name() string { return "worker-runtime" }

func (shell *Shell) Run(ctx context.Context) error {
	shell.logger.Info("worker shell ready", "resource_class", shell.resourceClass)
	<-ctx.Done()
	shell.once.Do(func() { close(shell.stopped) })
	return ctx.Err()
}

func (shell *Shell) Shutdown(context.Context) error {
	shell.once.Do(func() { close(shell.stopped) })
	return nil
}

func (shell *Shell) Stopped() <-chan struct{} { return shell.stopped }
