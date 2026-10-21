# bes-proxy

Durable BuildBuddy BES relay hosted by the [`workspace` daemon](../workspace-daemon/README.md).
Concurrent streams receive ordered local ACKs only after a Fjall `SyncAll`
transaction. Forwarding continues independently; matching upstream ACKs delete
payloads atomically with their replay checkpoints. Pending data is never LRU-evicted.

Streams stay open until their terminal event; disconnects cancel incomplete
attempts without finalizing them. Restart replays complete pending streams;
unfinished streams wait for their client to reconnect. Exhausted retries retain
the backlog for `workspace retry-bes`. Completed receipts have bounded retention.

See [`[bes]` configuration](../../examples/workspace.toml) and
[retry/timeout semantics](../../docs/reapi-policy.md#bes-is-a-separate-policy).
This is a BuildBuddy-targeted relay, not a generic lifecycle-ordering gateway.
