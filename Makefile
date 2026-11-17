.DEFAULT_GOAL := check

GO       ?= go
VERSIONS ?= 2.4.1 3.4.2 3.5.0

.PHONY: check
check: lint test ## Run lint and unit tests (what CI runs)

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: cover
cover: ## Run unit tests with coverage and print the summary
	$(GO) test -race -count=1 -coverprofile=coverage.out -covermode=atomic .
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: stress
stress: ## Run the concurrency stress tests many times
	$(GO) test -race -count=20 -run 'TestMultiJobStress|TestShutdownUnderLoad' .

.PHONY: bench
bench: ## Run benchmarks
	$(GO) test -run '^$$' -bench . -benchmem .

FUZZTIME ?= 30s
FUZZ_TARGETS = FuzzHandler FuzzChunkOf FuzzLogLine

.PHONY: fuzz
fuzz: ## Run every fuzz target for FUZZTIME (default 30s)
	@set -e; for f in $(FUZZ_TARGETS); do echo ">> $$f"; $(GO) test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) .; done

.PHONY: lint
lint: ## Run golangci-lint (v2)
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format code
	golangci-lint fmt ./...

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: e2e
e2e: ## Run end-to-end tests against a real xxl-job-admin (docker); XXL_JOB_VERSION=2.4.1|3.4.2|3.5.0
	./e2e/run.sh

.PHONY: e2e-all
e2e-all: ## Run end-to-end tests against every supported admin version
	@set -e; for v in $(VERSIONS); do XXL_JOB_VERSION=$$v ./e2e/run.sh; done

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
