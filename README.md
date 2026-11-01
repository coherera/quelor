# Quelor

**Workload admission and flow control for CI and agents. Stop starting work that cannot finish.**

[![License](https://img.shields.io/github/license/coherera/quelor)](LICENSE)
[![CI](https://github.com/coherera/quelor/actions/workflows/ci.yml/badge.svg)](https://github.com/coherera/quelor/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/coherera/quelor)](https://goreportcard.com/report/github.com/coherera/quelor)
[![Go Reference](https://pkg.go.dev/badge/github.com/coherera/quelor.svg)](https://pkg.go.dev/github.com/coherera/quelor)
[![Release](https://img.shields.io/github/v/release/coherera/quelor?include_prereleases)](https://github.com/coherera/quelor/releases)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/coherera/quelor/badge)](https://securityscorecards.dev/viewer/?uri=github.com/coherera/quelor)
[![Stars](https://img.shields.io/github/stars/coherera/quelor?style=social)](https://github.com/coherera/quelor/stargazers)
[![Conformance: candidate](https://img.shields.io/badge/conformance-candidate-lightgrey.svg)](docs/conformance.md)

Quelor is for CI owners and agent authors whose jobs die mid-run on rate limits, budgets or quotas. Read the [manifesto](https://quelor.dev/manifesto/).

## Try it in 2 minutes

You need Go ([version in go.mod](go.mod)) and a GitHub token.

```
go install github.com/coherera/quelor/cmd/quelor@latest
GITHUB_TOKEN=$(gh auth token) quelor admit --resource github_api --need 50
```

Real output from a run of this command:

```
admit github_api need=50 remaining=4823 reset=2026-11-01T09:31:10Z
```

Exit codes: `0` admit (enough budget), `42` defer (not enough budget and you passed `--optional`), `43` degraded (not enough budget for a required demand, or the reading is unknown, for example no token). Unknown is never treated as available. Add `--json` for machine output.

## The problem

CI pipelines and agent sessions start work against budgets they never check: API rate windows, runner slots, model credit windows, dollar caps, and dependencies that are already down. The result is a wall of red that is not defects, paged humans who cannot fix a 429, and reruns that spend the same empty bucket twice.

Quelor admits work against machine resources: API budgets, executor slots, model credit windows, dollar budgets, dependency health. Each resource is declared as a YAML manifest with one of five capacity models ([docs/resource-classes.md](docs/resource-classes.md)). The GitHub Action and `quelor admit` check one live GitHub rate-limit reading, with no daemon; `quelord` holds many resources and arbitrates between many holders.

## Quickstart (GitHub Action)

```yaml
- name: Admit before start
  id: gate
  uses: coherera/quelor@v0
  with:
    resource: github_api
    need: '300'        # calls the next step expects to make
    optional: 'true'   # defer (skip) instead of going red when short

- name: API-heavy step
  if: steps.gate.outputs.verdict == 'admit'
  run: ./scripts/chatty-step.sh
```

Add it as the first step of a chatty workflow. If the shared token budget is spent or GitHub cannot be read, the step defers with a reason instead of failing, and your checks stay honest. Outputs: `verdict` (`admit`, `defer`, `degraded`) and `remaining`. A full example is in [examples/workflows/chatty-ai-step.yml](examples/workflows/chatty-ai-step.yml).

The same check runs anywhere, with no daemon:

```
$ GITHUB_TOKEN=$(gh auth token) quelor admit --resource github_api --need 50
admit github_api need=50 remaining=4950 reset=2026-10-31T15:00:00Z
```

Exit codes are as in "Try it in 2 minutes" above. The Action sets the `verdict` and `remaining` outputs itself and does not fail the job on `defer` or `degraded` unless you set `fail-on-defer: 'true'`; gate later steps on `steps.gate.outputs.verdict`. If no release binary is available the Action falls back to `go install`, which needs Go on the runner.

**Verdict names.** The engine's lease verdicts (`ADMIT`, `DEFER`, `SHED`, `DENY`; [docs/architecture.md](docs/architecture.md)) and the Action/CLI verdicts (`admit`, `defer`, `degraded`) are separate vocabularies, because the CLI does not run the engine. `admit` and `defer` mean what `ADMIT` and `DEFER` mean; `degraded` is a refusal of a required demand or an unknown reading and has no exact engine twin (closest: `DENY`). `SHED` (dropped by coalescing) exists only in the engine.

## What it is

- A **lease API**: `AcquireLease`, `RenewLease`, `SettleLease`. Admit, renew, settle, classify. See [docs/architecture.md](docs/architecture.md).
- `quelord`: a single deterministic daemon. Floors and caps, coalescing, AIMD on observed 429/503, progressive renewal for long sessions, scarcest-resource tickets with aging, per-holder reservation quotas, authenticated holders. In one line each:
  - **AIMD** (additive increase, multiplicative decrease): an upstream 429 or 503 halves a resource's effective capacity; each success raises it by 5 percent of declared capacity ([architecture](docs/architecture.md#capacity-models-and-feedback)).
  - **Coalescing**: units that share a `coalesce_key` do not run twice; the newcomer or the older run is shed ([architecture](docs/architecture.md)).
  - **Tickets**: contention on a scarce resource is queued by priority rank, and rank improves with waiting (`aging_step`) so low-priority work is not starved.
  - **Progressive renewal**: a long session holds a short lease and renews it a quantum at a time; a refused renewal tells the holder to checkpoint (`Hibernate`) instead of dying mid-run.
- A **five-class outcome classifier**: `resource-exhausted`, `dependency-unavailable`, `flaky`, `real-defect`, `unknown`. Only `real-defect` goes red. `quelor classify` ships a default ruleset for GitHub Actions failures.
- A **pluggable Decider** interface ([docs/decider-interface.md](docs/decider-interface.md)) with a rules-first reference Decider (YAML + CEL, [docs/policy-cel.md](docs/policy-cel.md)), content-addressed rulesets, and log-only shadow slots.
- **Replay** (`quelor replay --trace t.jsonl --decider ref.yaml`) and a **conformance suite** ([docs/conformance.md](docs/conformance.md)), so you can prove your own Decider or engine.
- A **decision trace** as JSONL with a published schema ([schema/trace.v1.json](schema/trace.v1.json)) and optional OpenTelemetry spans.
- Benchmarks with published methodology ([docs/benchmarks.md](docs/benchmarks.md)): `go run ./cmd/quelor-bench`.

## What it is not

- Not a durable execution engine. Temporal, Hatchet, Inngest and friends run already-admitted work.
- Not a CI runner or a merge queue.
- Not a per-token LLM gateway. Gateways enforce per call; Quelor admits per unit and renews per quantum.
- Not a queue you must hand your payloads and credentials to. The default mode keeps both with you.
- Not a model on your outage path. The hot path is deterministic. Always.

## Run the daemon

```
go install github.com/coherera/quelor/cmd/quelor@latest
go install github.com/coherera/quelor/cmd/quelord@latest
quelord -config examples/quelord.yaml
```

shared_token may be given directly or read from an environment variable via shared_token_env. Per-holder tokens may be given as a plaintext holder_tokens map, read from a YAML file via holder_tokens_file, or read from an environment variable via holder_tokens_env (JSON object or holder:token comma list). Precedence, lowest to highest: plaintext map, file, environment. Tokens are never logged.

[examples/quelord.yaml](examples/quelord.yaml) is a minimal valid config (two resources, listening on 127.0.0.1:8080); `curl 127.0.0.1:8080/healthz` confirms it is up.

Endpoints: `POST /v1/lease/acquire`, `/v1/lease/renew`, `/v1/lease/settle`, `/v1/observe`, `GET /healthz`, `GET /metrics`. Resource classes are declared as YAML manifests ([docs/resource-classes.md](docs/resource-classes.md)).

## Status

`0.x`. Envelope and verdict schemas freeze at `1.0` after a production census. Four independent version lines apply: lease API, envelope schema, trace schema, Decider interface. Pricing for hosted cells is unset; the open engine is complete on its own.

## Where this leads

Hosted multi-tenant cells from ARAMAI. Nothing in this repository depends on them.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions are accepted under the Developer Certificate of Origin (`git commit -s`). Security reports: [SECURITY.md](SECURITY.md). Governance: [GOVERNANCE.md](GOVERNANCE.md). Trademarks: [TRADEMARKS.md](TRADEMARKS.md). Starter work: [docs/good-first-issues.md](docs/good-first-issues.md).

---

Quelor is built and maintained by [ARAMAI](https://aramai.net). Apache-2.0.
