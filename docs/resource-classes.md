# Resource classes

A resource is a scarce thing a unit of work demands: API calls, executor slots, a dollar
budget, a dependency's health. Each is declared as a YAML manifest with one of five
capacity models. Source: `pkg/resource/manifest.go` (schema and validation) and
`pkg/engine/state.go` (behavior).

## Manifest

```yaml
resources:
  - id: github_api
    capacity_model: window_bucket
    capacity: 5000
    window: 1h
  - id: runner_slots
    capacity_model: lease
    capacity: 8
  - id: llm_spend_cents
    capacity_model: hard_budget
    capacity: 20000
    unknown_policy: deny
  - id: payments_api
    capacity_model: circuit
    failure_threshold: 3
    open_for: 30s
```

A file may hold one manifest, a top-level `resources:` list, or several `---` separated
documents. Unknown fields are rejected. `quelord` loads them inline (`resources:`) or
from files (`resource_files:`).

| Field | Meaning | Default |
| --- | --- | --- |
| `id` | Resource id, the key used in `Envelope.Demands`. Required. | |
| `capacity_model` | One of the five models below. Required. | |
| `capacity` | Declared capacity. Must be above 0, except `circuit`, where 0 is allowed. | |
| `window` | Duration string. Required above 0 for `gcra` and `window_bucket`. | |
| `freshness_ttl` | A reading older than this is stale. 0 means never stale. | 0 |
| `unknown_policy` | What to do when a reading is unknown or stale. | `defer_short` |
| `min_capacity` | Floor for AIMD shrinking. | 1 |
| `failure_threshold` | Consecutive failures that open a circuit. | 3 |
| `open_for` | How long an open circuit stays open before a probe. | 30s |

## Capacity models

| Model | Behavior |
| --- | --- |
| `lease` | Concurrent slots. A lease holds its declared quantity until settle or expiry, then releases all of it. Available is effective capacity minus in-use. |
| `hard_budget` | A spend counter that never refills. Settle refunds only `declared - consumed`. Available is effective capacity minus spent. |
| `window_bucket` | Like a budget, but spent resets to 0 each `window`. A settle refunds unused quantity only if the lease was taken in the current window. |
| `gcra` | A token bucket refilled continuously: `capacity` tokens per `window`. Unused tokens are refunded on settle, capped at capacity. |
| `circuit` | A dependency breaker. Closed admits freely. After `failure_threshold` consecutive failures (HTTP 5xx or 429 observed) it opens and refuses for `open_for`, then goes half-open and admits one probe. A successful probe closes it; a failed one reopens it. Observations while open do not close it. |

AIMD applies to `lease`, `hard_budget`, `window_bucket` and `gcra`. HTTP 429 or 503
halves the effective capacity (never below `min_capacity`); each 2xx or 3xx observation
raises it by `capacity / 20` (at least 1) up to the declared capacity. A `Retry-After` on
the observation blocks the resource until it elapses. Circuits do not use AIMD.

## Unknown is never available

A reading is usable only if `known` and not `stale`. When it is not usable, the
manifest's `unknown_policy` decides (`UnknownPolicy.Verdict`):

| Policy | Verdict |
| --- | --- |
| `defer_short` | `DEFER` |
| `assume_busy` | `DEFER` |
| `hold_cursor` | `ADMIT`. Fail-open; only for resources the engine tracks itself. |
| `deny` | `DENY` |

`hard_budget` with `hold_cursor` is rejected at validation (`ErrFailOpenSpend`): a spend
budget must never admit on an unknown reading. The same rule holds when the Decider is
unreachable: a hard budget is refused even for an operator directive.

## Where readings come from

A resource registered without a reader is engine-local: the engine reports its own
accounting as a known reading. A resource registered with a `resource.Reader` (a probe
of an external system, `pkg/resource/reader.go`) gets a reading stamped with age and
freshness; a probe error yields an unknown reading and `available` is forced to 0.

`quelord` registers every manifest without a reader (`Config.Build` in
`pkg/server/config.go` states no external readers are attached), so `freshness_ttl` and
`unknown_policy` matter when you embed the engine and pass readers to
`resource.Registry.Register`. For live external readings without a daemon,
`quelor admit` reads GitHub's rate limits (`github_api`, `github_search`,
`github_graphql`, `github_code_search`) from `/rate_limit`, or Docker Hub's pull
budget (`dockerhub_pulls`) from its registry rate-limit headers. Both are read
live per call; an unknown reading is never available capacity.

## Runner slots

[runner-slots.yaml](../examples/manifests/runner-slots.yaml) declares hosted,
macOS and GPU concurrent slots with the existing `lease` model. Its capacities
are examples, not discovered limits: set them from your own GitHub plan and
runner pool. Load it through `resource_files` in the `quelord` config.

Use the daemon's lease HTTP API (or embed the engine) to acquire one unit before
starting participating work, renew before expiry while it runs, and settle when
it stops. Stop work if renewal is refused; an expired lease releases its slots
even if the work continues. A hosted macOS job should demand both
`gha_hosted_jobs: 1` and `gha_macos_jobs: 1` in the same acquire so both limits
apply. Demand `gpu_runner_slots: 1` for work using that pool.

Quelor cannot read GitHub's live concurrency. It counts only jobs that take and
maintain a lease from the same `quelord` instance; other jobs are invisible.
A step inside a GitHub Actions job runs after its runner is assigned, so gating
that step controls cooperative work, not GitHub's scheduling queue. Controlling
submission itself requires a caller that acquires before dispatching the job.
The daemon-free `quelor admit` CLI and Action do not acquire these slot leases.

## Extending

Resource classes model machine resources only. To add a resource, write a manifest. To
add a capacity model you change `pkg/engine/state.go` and `pkg/resource/manifest.go`;
see [CONTRIBUTING.md](../CONTRIBUTING.md).
