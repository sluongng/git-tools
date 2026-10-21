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
`post_half_close_timeout_ms` bounds each forwarding attempt's upstream completion
wait after request EOF (default five minutes; `0` disables; maximum seven days).
It covers response headers and final ACKs/EOF, not ACK inactivity while sending,
so upstreams may buffer ACKs until EOF. Expiry uses normal suffix retry; exhausted
retries release the forwarding slot and retain unacknowledged data for retry.
See [retry/timeout policy](../../docs/reapi-policy.md).
This is a BuildBuddy-targeted relay, not a generic lifecycle-ordering gateway.
