package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	workerruntime "github.com/uu999/evalfrog/internal/worker/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(workerruntime.RunProcess(ctx, os.Args[1:], "builtin", os.Stdout, os.Stderr))
}
