# syntax=docker/dockerfile:1

FROM node:24-alpine AS ui-builder
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# Build stage: compile all four binaries (CGO-free) with version LDFLAGS.
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
COPY --from=ui-builder /ui/dist ./ui/dist

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
ENV LDFLAGS="-X github.com/nunocgoncalves/control-plane/internal/version.version=${VERSION} -X github.com/nunocgoncalves/control-plane/internal/version.commit=${COMMIT} -X github.com/nunocgoncalves/control-plane/internal/version.date=${DATE}"

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/manager ./cmd/manager && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$LDFLAGS" -o /out/dispatch ./cmd/dispatch

# Runtime stage: one image, four binaries. Each Deployment selects its binary
# via `command` (manager: ["/manager"]; api: ["/api", "serve"]; gateway: ["/gateway", "serve"]; dispatch: ["/dispatch", "serve"]).
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/manager /manager
COPY --from=builder /out/api /api
COPY --from=builder /out/gateway /gateway
COPY --from=builder /out/dispatch /dispatch
USER 65532:65532
