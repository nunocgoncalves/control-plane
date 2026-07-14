# Image URL to use for building/pushing image targets (matches config/manager).
IMG ?= controller:latest

# Setting SHELL to bash allows bash commands to be executed by recipes.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
CONTAINER_TOOL ?= docker

# Version metadata injected into binaries via linker flags.
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X github.com/nunocgoncalves/control-plane/internal/version.version=$(VERSION) \
           -X github.com/nunocgoncalves/control-plane/internal/version.commit=$(COMMIT) \
           -X github.com/nunocgoncalves/control-plane/internal/version.date=$(DATE)

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd:allowDangerousTypes=true webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if gofmt would change anything.
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test-unit
test-unit: ## Run unit tests (skips integration/Docker tests via -short).
	go test -short -race -count=1 ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests (unit + envtest + integration).
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test ./... -coverprofile cover.out

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run ./...

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix ./...

##@ Build

.PHONY: build
build: vet ## Build manager and api binaries.
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/manager ./cmd/manager
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/api ./cmd/api

.PHONY: run-manager
run-manager: ## Run the operator from your host.
	go run ./cmd/manager

.PHONY: run-api
run-api: ## Run the API (serve) from your host. Requires DATABASE_URL.
	go run ./cmd/api serve

.PHONY: migrate-up
migrate-up: ## Apply database migrations.
	go run ./cmd/api migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the last N migrations (default: all).
	go run ./cmd/api migrate down

.PHONY: docker-build
docker-build: ## Build docker image with both binaries.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image.
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment (dev/envtest only — prod is forge Helm)

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster in ~/.kube/config.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Harness (Node pi harness — HOR-351)

HARNESS_IMG ?= control-plane-harness:latest
BUF ?= buf

.PHONY: proto-tools
proto-tools: ## Install buf + protoc plugins (proto lint + codegen). buf: brew install buf (or GOBIN=$$PWD/bin go install github.com/bufbuild/buf/cmd/buf@latest).
	@command -v $(BUF) >/dev/null 2>&1 || GOBIN="$(LOCALBIN)" go install github.com/bufbuild/buf/cmd/buf@latest
	@command -v protoc-gen-go >/dev/null 2>&1 || GOBIN="$(LOCALBIN)" go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@command -v protoc-gen-connect-go >/dev/null 2>&1 || GOBIN="$(LOCALBIN)" go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	@command -v protoc-gen-es >/dev/null 2>&1 || npm install -g @bufbuild/protoc-gen-es
	@PATH="$(LOCALBIN):$$PATH" $(BUF) --version

.PHONY: proto
proto: proto-tools ## Generate Go (internal/harnessrpc) + TS (harness/src/gen) stubs from proto/.
	cd proto && PATH="$(LOCALBIN):$$PATH" $(BUF) lint && PATH="$(LOCALBIN):$$PATH" $(BUF) generate

.PHONY: proto-check
proto-check: proto ## CI guard: proto lints clean and generated code is fresh (regenerate + diff).
	@git diff --exit-code -- internal/harnessrpc harness/src/gen || { \
		echo "generated code is stale; run 'make proto' and commit"; exit 1; }

.PHONY: harness-deps
harness-deps: ## Install harness Node deps (npm install).
	cd harness && npm install

.PHONY: harness-build
harness-build: ## Build the harness (tsc -> dist).
	cd harness && npm run build

.PHONY: harness-test
harness-test: ## Run harness unit + integration tests (vitest).
	cd harness && npm test

.PHONY: harness-lint
harness-lint: ## Typecheck the harness (tsc --noEmit).
	cd harness && npm run lint

.PHONY: harness-image
harness-image: ## Build the harness container image.
	$(CONTAINER_TOOL) build -t $(HARNESS_IMG) -f harness/Dockerfile harness/

##@ Tooling

.PHONY: install-hooks
install-hooks: ## Install git hooks (.githooks/pre-commit).
	git config core.hooksPath .githooks

.PHONY: clean
clean: ## Remove built binaries and test caches.
	rm -rf bin/ dist/ cover.out
	go clean -testcache

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

# setup-envtest is its own module (sigs.k8s.io/controller-runtime/tools/setup-envtest),
# not versioned with controller-runtime; @latest resolves it. Pin a specific
# setup-envtest release here if you need reproducibility.
ENVTEST_VERSION ?= latest

# ENVTEST_K8S_VERSION is derived from k8s.io/api in go.mod (e.g. 1.36).
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download envtest binaries for the configured Kubernetes version.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
