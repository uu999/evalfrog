package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uu999/evalfrog/internal/platform/buildinfo"
)

type Registry struct {
	prometheus *prometheus.Registry
	Requests   *prometheus.CounterVec
}

func New(service string) *Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	build := buildinfo.Current()
	buildGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "evalfrog",
		Name:      "build_info",
		Help:      "Static build metadata for the running EvalFrog process.",
	}, []string{"service", "version", "commit"})
	buildGauge.WithLabelValues(service, build.Version, build.Commit).Set(1)
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "evalfrog",
		Name:      "http_requests_total",
		Help:      "HTTP requests handled by the M0 process shell.",
	}, []string{"service", "route", "status"})
	registry.MustRegister(buildGauge, requests)
	return &Registry{prometheus: registry, Requests: requests}
}

func (registry *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(registry.prometheus, promhttp.HandlerOpts{})
}
