.DEFAULT_GOAL := check

GO       ?= go
VERSIONS ?= 2.4.1 3.3.1 3.4.2 3.5.0
# Go modules in this repository: the executor and the OpenTelemetry integration.
MODULES  := . otelxxljob

.PHONY: check
check: lint test ## Run lint and unit tests for every module (what CI runs)

.PHONY: test
test: ## Run unit tests with the race detector
	@set -e; for m in $(MODULES); do (cd $$m && $(GO) test -race -count=1 ./...); done

.PHONY: cover
cover: ## Run the executor's unit tests with coverage and print the summary
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
# The Go fuzz coordinator can report the end of -fuzztime itself as "context deadline exceeded" (it reads the
# worker context before the deadline has propagated to it). Such a run found nothing: no input is saved and the
# only error is that line. Any other failure, or a saved failing input, fails the target.
fuzz: ## Run every fuzz target for FUZZTIME (default 30s)
	@for f in $(FUZZ_TARGETS); do echo ">> $$f"; \
		out=$$($(GO) test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . 2>&1) && { echo "$$out" | tail -1; continue; }; \
		echo "$$out" | grep -v '^fuzz: elapsed'; \
		if ! echo "$$out" | grep -q 'Failing input' && \
			[ "$$(echo "$$out" | grep -E '^    ' | sed 's/^ *//')" = "context deadline exceeded" ]; then \
			echo "(ignored: the fuzzing deadline itself, reported by the Go toolchain)"; continue; fi; \
		exit 1; done

.PHONY: lint
lint: ## Run golangci-lint (v2) on every module
	@set -e; for m in $(MODULES); do (cd $$m && golangci-lint run ./...); done

.PHONY: fmt
fmt: ## Format code
	@set -e; for m in $(MODULES); do (cd $$m && golangci-lint fmt ./...); done

.PHONY: tidy
tidy: ## Run go mod tidy in every module
	@set -e; for m in $(MODULES); do (cd $$m && $(GO) mod tidy); done

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	@set -e; for m in $(MODULES); do (cd $$m && $(GO) run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...); done

.PHONY: e2e
e2e: ## Run end-to-end tests against a real xxl-job-admin (docker); XXL_JOB_VERSION=2.4.1|3.3.1|3.4.2|3.5.0
	./e2e/run.sh

.PHONY: e2e-all
e2e-all: ## Run end-to-end tests against every supported admin version
	@set -e; for v in $(VERSIONS); do XXL_JOB_VERSION=$$v ./e2e/run.sh; done

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
