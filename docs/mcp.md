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
| `--call-timeout <dur>` | `10m` | Longest a `tools/call` may stay in flight. After that the engine expires its lease and frees its concurrency slot, so a call the server never answers cannot hold a slot forever. The rate token is not refunded: the call was admitted, so it counts against the rate window until the window refills. Set it above your slowest tool. |
| `--coalesce` | off (opt-in) | Shed a duplicate of an in-flight call with the same tool name and canonical JSON arguments (`SHED`, `coalesced:drop-new`). Without this flag no call is ever shed. |
| `--trace <file.jsonl>` | off | Append Quelor decision trace records (`trace.v1` JSONL) to a file. |

## Behavior

- **Pass-through:** `initialize`, `tools/list`, notifications and responses are forwarded verbatim without modification. A client line that is not exactly one JSON value (invalid JSON, two objects on one line, invalid UTF-8, or a carriage return anywhere but directly before the final newline) is refused with a `-32700` parse error and never forwarded. A CRLF line ending is accepted.
- **Refusals:** When a `tools/call` is not admitted (`DEFER` or `SHED`), it is not sent to the child. The proxy replies directly to the client with an MCP tool result containing `isError: true` and text naming the verdict, reason, and `wake_at` when known.
- **No ungated `tools/call`:** every `tools/call` is either gated or refused; none reaches the child unchecked.
  - Every JSON-RPC **batch** (a JSON array) is refused with `-32600`; MCP removed batching in the 2025-06-18 spec. Send each message on its own line.
  - A `method` holding any character other than letters, digits and `_ / . $ -` is refused with `-32600`.
  - A `tools/call` whose **params do not parse** is refused with `-32602`. One that is valid JSON but not a valid request envelope is refused with `-32600`.
  - A `tools/call` with **no id** (or a null id) is a notification and cannot be answered, so it is dropped.
  - With `--coalesce`, a call whose arguments cannot be canonicalized is refused with `-32602`.
  - Each of these is logged on stderr as a `REFUSE` line.
- **Strict envelope:** the proxy reads `jsonrpc`, `id`, `method` and `params` (and `name` and `arguments` inside `params`) only from those exact lowercase keys, because servers differ from Go's case-insensitive, last-key-wins JSON reading. A line is refused with `-32600` and not forwarded when any two keys of the request object, or of a `tools/call` `params` object, are equal under case folding (`method` and `METHOD`, or `method` twice), or when one of those reserved keys appears only in another case (`METHOD` or `Method` alone; `-32602` inside `params`). A method that equals `tools/call` only under case folding is refused too. A `method` that is present but empty or not a string is refused with `-32600`. Any request (a message with a `method`) whose `id` belongs to a `tools/call` still in flight is refused with `-32600`, so its reply cannot settle the running call. Responses the client sends to the server's own requests are not checked, because those ids are a separate space. A `tools/call` is also refused when `params` is missing, null or not an object, or when `name` is missing, empty or not a string (`-32602`). A refusal echoes the request `id` when it is a string or number at the exact key `id`, and `null` otherwise. Numeric ids are matched by value, so `1` and `1.0` are the same id. A `notifications/cancelled` whose params have colliding keys is forwarded but settles nothing.
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
