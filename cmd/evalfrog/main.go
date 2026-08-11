package main

import (
	"context"
	"os"

	"github.com/uu999/evalfrog/internal/cli"
)

func main() {
	os.Exit((cli.App{Output: os.Stdout, Error: os.Stderr}).Run(context.Background(), os.Args[1:]))
}
