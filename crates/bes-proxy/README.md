# bes-proxy

Library providing the [`workspace`](../workspace-daemon/README.md) daemon's durable
BuildBuddy BES relay. Multiple streams receive ordered ACKs only after durable
local persistence; forwarding proceeds independently.

Upstream ACKs atomically remove payloads and advance replay checkpoints.
Pending data is never LRU-evicted. Restart replays complete streams; unfinished
streams wait for reconnection. Exhausted retries retain the backlog for
`workspace retry-bes`. Keep the state directory across restarts.

Configure batching, limits and upstream settings in [`[bes]`](../../examples/workspace.toml).
Setting `batch_max_events = 1` disables grouping, not durability.
See [retry/timeout policy](../../docs/reapi-policy.md).
This is a BuildBuddy-targeted relay, not a generic lifecycle-ordering gateway.
