package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/platform/buildinfo"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type App struct {
	Output io.Writer
	Error  io.Writer
}

func (app App) Run(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		app.usage()
		return 2
	}
	switch arguments[0] {
	case "version":
		value, _ := json.Marshal(buildinfo.Current())
		fmt.Fprintln(app.Output, string(value))
		return 0
	case "config":
		return app.config(arguments[1:])
	case "doctor":
		return app.doctor(ctx, arguments[1:])
	default:
		fmt.Fprintf(app.Error, "unknown command %q\n", arguments[0])
		app.usage()
		return 2
	}
}

func (app App) config(arguments []string) int {
	if len(arguments) == 0 || arguments[0] != "validate" {
		fmt.Fprintln(app.Error, "usage: evalfrog config validate [--profile PROFILE] [--config-dir DIR]")
		return 2
	}
	flags := flag.NewFlagSet("config validate", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	profile := flags.String("profile", "", "configuration profile")
	directory := flags.String("config-dir", "", "configuration directory")
	if err := flags.Parse(arguments[1:]); err != nil {
		return 2
	}
	value, err := config.Load(config.LoadOptions{Directory: *directory, Profile: *profile})
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	fmt.Fprintf(app.Output, "configuration valid: profile=%s namespace=%s\n", value.Profile, value.Namespace)
	return 0
}

func (app App) doctor(ctx context.Context, arguments []string) int {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	profile := flags.String("profile", "", "configuration profile")
	directory := flags.String("config-dir", "", "configuration directory")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	value, err := config.Load(config.LoadOptions{Directory: *directory, Profile: *profile})
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, strings.TrimRight(value.Endpoints.ControlPlaneURL, "/")+"/health/ready", nil)
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(app.Error, "control plane is not ready: %s\n", response.Status)
		return 1
	}
	fmt.Fprintln(app.Output, "EvalFrog local stack is ready")
	return 0
}

func (app App) usage() {
	fmt.Fprintln(app.Error, "usage: evalfrog <version|config validate|doctor>")
}
