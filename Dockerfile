FROM golang:1.26-alpine AS build

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for target in evalfrog control-plane worker-builtin worker-sandbox; do \
      CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w -X github.com/uu999/evalfrog/internal/platform/buildinfo.Version=${VERSION} -X github.com/uu999/evalfrog/internal/platform/buildinfo.Commit=${COMMIT} -X github.com/uu999/evalfrog/internal/platform/buildinfo.Date=${BUILD_DATE}" \
        -o "/out/${target}" "./cmd/${target}"; \
    done

FROM scratch AS runtime-base
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY configs /app/configs
COPY migrations /app/migrations
USER 65532:65532

FROM runtime-base AS evalfrog
COPY --from=build /out/evalfrog /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS control-plane
COPY --from=build /out/control-plane /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS worker-builtin
COPY --from=build /out/worker-builtin /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS worker-sandbox
COPY --from=build /out/worker-sandbox /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]
