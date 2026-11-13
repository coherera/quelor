# Contributing to Quelor

Thanks for helping. Quelor admits work against machine resources; keep changes
small and tested.

## Dev loop

Go 1.26 or newer (`go 1.26.0` in `go.mod`).

```
go build ./... && go test ./... && go vet ./...
go test -bench . ./bench/
```

## Rules

* Every behaviour change has a test. A new control must be seen failing before
  it is trusted: include a test that fails without your change.
* Deterministic engine only on the hot path: no network or model calls in
  `pkg/engine`.
* Unknown is never available. Do not add a code path that treats a stale or
  missing reading as capacity.
* Scope: resource classes model machine resources only.

## Sign-off (DCO)

Every commit must carry a `Signed-off-by` line certifying the
[Developer Certificate of Origin](https://developercertificate.org/):

```
git commit -s -m "your message"
```

## Response times

We aim to respond to a new issue or pull request within **2 days** and to
triage it (label, accept, decline, or ask for more information) within
**5 days**.

## Pull requests

Open a PR whenever you like, draft or not. A sentence or two on what and why is all the description we need. A maintainer reviews; CI must pass.
Good first issues are listed in [docs/good-first-issues.md](docs/good-first-issues.md).
