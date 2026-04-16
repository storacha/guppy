# Build stage — native platform for fast cross-compilation
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build

ARG TARGETARCH
ARG TARGETOS=linux
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
ARG BUILT_BY=docker

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w \
      -X github.com/storacha/guppy/pkg/build.version=${VERSION} \
      -X github.com/storacha/guppy/pkg/build.Commit=${COMMIT} \
      -X github.com/storacha/guppy/pkg/build.Date=${DATE} \
      -X github.com/storacha/guppy/pkg/build.BuiltBy=${BUILT_BY}" \
    -o /app .

# Runtime stage — alpine ships wget, CA certs, /bin/sh, and the nobody user
FROM alpine:latest AS prod

USER nobody

COPY --from=build /app /usr/bin/guppy

ENTRYPOINT ["/usr/bin/guppy"]
