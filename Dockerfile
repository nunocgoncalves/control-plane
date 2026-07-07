# syntax=docker/dockerfile:1

# Build stage: compile both binaries (CGO-free) with version LDFLAGS.
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
ENV LDFLAGS="-X github.com/nunocgoncalves/control-plane/internal/version.version=${VERSION} -X github.com/nunocgoncalves/control-plane/internal/version.commit=${COMMIT} -X github.com/nunocgoncalves/control-plane/internal/version.date=${DATE}"

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/manager ./cmd/manager && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/api ./cmd/api

# Runtime stage: one image, two binaries. Each Deployment selects its binary
# via `command` (manager: ["/manager"]; api: ["/api", "serve"]).
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/manager /manager
COPY --from=builder /out/api /api
USER 65532:65532
