# Contributing

Thanks for taking the time to contribute! Bug reports, fixes, docs and tests are all welcome.

## Before you start

- For anything beyond a small fix, please open an issue first so we can agree on the approach.
- The core package depends only on the Go standard library. Please keep it that way; integrations with frameworks,
  metrics or tracing libraries belong in your application or in a separate module.
- The executor must keep matching the behaviour of the official Java executor. If you change protocol handling or
  block-strategy semantics, explain the reference behaviour in the pull request.

## Development

Requirements: Go 1.24+, [golangci-lint v2](https://golangci-lint.run/), and Docker with compose v2 for the
end-to-end tests.

```bash
make check    # golangci-lint + unit tests with -race (what CI runs)
make cover    # coverage summary
make stress   # repeat the concurrency stress tests
make fuzz     # run each fuzz target for FUZZTIME (default 30s)
make bench    # benchmarks
make e2e      # end-to-end tests against a real admin (XXL_JOB_VERSION=2.4.1|3.4.2|3.5.0)
make e2e-all  # all supported admin versions
```

## Guidelines

- **Tests.** New behaviour needs tests. Tests use [testify](https://github.com/stretchr/testify) and start with a
  short `// Scenario:` / `// Given:` / `// Expect:` comment. Concurrency changes should keep `make stress` green.
- **Style.** `make lint` must pass. Exported identifiers need doc comments; comments and docs are in English.
- **Compatibility.** Public API changes must be documented in the godoc, the README (both languages) and
  `CHANGELOG.md`.
- **Commits.** Keep commits focused and write messages in the imperative mood ("Add …", "Fix …").

## Repository layout

Read [ARCHITECTURE.md](ARCHITECTURE.md) first: every invariant of the executor has one owner, and a change should
keep it there rather than spread it.

The repository holds two Go modules: the executor at the root (with its internal packages) and the OpenTelemetry
integration in [`otelxxljob/`](otelxxljob). `otelxxljob/go.mod` uses a `replace` directive to build against the
executor in this repository, so changes to both can land in one pull request. `make test`, `lint`, `fmt`, `tidy` and
`vuln` run on every module; `cover`, `stress`, `bench`, `fuzz` and the e2e targets cover the executor.

## Releasing (maintainers)

Both modules are released together with the same version (`vX.Y.Z` for the executor, `otelxxljob/vX.Y.Z` for the
integration):

1. Move the `Unreleased` entries in `CHANGELOG.md` under the new version, and make sure CI on `main` is green, end to
   end tests included. Tags cannot be moved or deleted once pushed (the Go proxy caches them, and a ruleset protects
   them), so nothing after this step can stop a broken release.
2. Tag the executor and push the tag: `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`. The release workflow
   records the GitHub release.
3. In `otelxxljob/`, require the new executor version (`go get github.com/ShawineWu/go-xxljob@vX.Y.Z`, keeping the
   `replace`), and check that it builds against the released executor rather than the repository's:
   `go mod edit -dropreplace github.com/ShawineWu/go-xxljob && go test ./... && git checkout go.mod go.sum`. Commit,
   wait for CI, then tag and push `otelxxljob/vX.Y.Z`.

## Reporting security issues

Please do not open public issues for vulnerabilities; see [SECURITY.md](SECURITY.md).
