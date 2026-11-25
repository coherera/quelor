# `quelor mcp`

`quelor mcp` is a stdio JSON-RPC 2.0 admission proxy for any Model Context Protocol (MCP) server. It spawns the target server as a child process, forwards non-`tools/call` traffic in both directions verbatim, and gates every client `tools/call` request through Quelor's embedded engine. It runs in-process: there is no daemon.

## Usage

```sh
quelor mcp [flags] -- <server-cmd> [args...]
```

The `--` separator is required before `<server-cmd>`. Everything after `--` is passed to the child process unchanged. If the child exits, `quelor mcp` exits with the child's exit code. A child killed by a signal exits with `128 + signal` (for example 137 for SIGKILL). On SIGINT or SIGTERM, `quelor mcp` stops the child and exits non-zero.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--rate <n>/<dur>` | `30/1m` | Maximum `tools/call` requests per window (`window_bucket` resource `mcp_rate`). The window refills while the proxy runs. |
| `--max-concurrent <n>` | `4` | Maximum in-flight `tools/call` requests (`lease` resource `mcp_concurrent`). |
| `--call-timeout <dur>` | `10m` | Longest a `tools/call` may stay in flight. After that the engine expires its lease and frees its rate and concurrency capacity, so a call the server never answers cannot hold a slot forever. Set it above your slowest tool. |
| `--coalesce` | off (opt-in) | Shed a duplicate of an in-flight call with the same tool name and canonical JSON arguments (`SHED`, `coalesced:drop-new`). Without this flag no call is ever shed. |
| `--trace <file.jsonl>` | off | Append Quelor decision trace records (`trace.v1` JSONL) to a file. |

## Behavior

- **Pass-through:** `initialize`, `tools/list`, notifications, responses, and malformed client lines are forwarded verbatim without modification.
- **Refusals:** When a `tools/call` is not admitted (`DEFER` or `SHED`), it is not sent to the child. The proxy replies directly to the client with an MCP tool result containing `isError: true` and text naming the verdict, reason, and `wake_at` when known.
- **No ungated `tools/call`:** every `tools/call` is either gated or refused; none reaches the child unchecked.
  - A JSON-RPC **batch** (a JSON array) that contains a `tools/call` is refused with a JSON-RPC error (`-32600`, "batches containing tools/call are not supported"); send each call as its own message. Batches without a `tools/call` pass through.
  - A `tools/call` whose **params do not parse** is refused with `-32602`. One that is valid JSON but not a valid request envelope is refused with `-32600`.
  - A `tools/call` with **no id** (or a null id) is a notification and cannot be answered, so it is dropped.
  - With `--coalesce`, a call whose arguments cannot be canonicalized is refused with `-32602`.
  - Each of these is logged on stderr as a `REFUSE` line.
- **In-flight accounting:** a call's lease is settled when the child replies, when the client sends `notifications/cancelled` for its request id (the notification is still forwarded to the child), or when the session ends (child exit, client EOF, signal). A call the child never answers is also expired after `--call-timeout`.
- **429 back-off and recovery:** `mcp_rate` capacity is adaptive (AIMD). When a reply is an error, or has `isError: true`, and `pkg/classify` classifies it with a rate-limit rule (`gha-rate-limit` or `gha-rate-limit-text`: HTTP 429, "rate limit exceeded", `Retry-After`), the settle carries a 429 observation and the effective rate halves. Every successful reply carries a 2xx observation and raises it again, up to the `--rate` ceiling. Other tool errors (a quota wall, out of memory, a plain failure) do not change capacity. The two rule ids come from the bundled default ruleset; despite the `gha-` prefix their wording is generic HTTP and API rate-limit text.
- **Logging:** Stdout carries only protocol bytes. Each gated `tools/call` logs one line to stderr, with the tool name quoted so a hostile name cannot forge a log line:
  `[quelor] ADMIT|SHED|DEFER id=<id> tool="<name>" reason=<reason>`
- **Trace file:** with `--trace`, each decision is a `trace.v1` JSONL record that `quelor replay --trace <file> --decider <decider.yaml>` can read; `--decider` is required. A failed call's record carries up to 512 bytes of the tool's error text, so treat the trace file as sensitive if your tools put secrets in their error messages.

## Example

Stderr from a real run against a stub MCP server with `--rate 2/1m --coalesce`: call 2 repeats call 1 while it is in flight, and call 4 exceeds the rate:

```text
[quelor] ADMIT id=1 tool="search_code" reason=ok
[quelor] SHED id=2 tool="search_code" reason=coalesced:drop-new
[quelor] ADMIT id=3 tool="search_code" reason=ok
[quelor] DEFER id=4 tool="search_code" reason=insufficient:mcp_rate
```
