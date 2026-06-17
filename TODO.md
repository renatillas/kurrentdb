# TODO

## Streaming-first backend

- Add a true HTTP/2 streaming abstraction to `backends/kurrentdb_erlang`. Initial raw stream API is in place.
- Use one `gun` connection per opened stream initially.
- Run stream actors under a `gleam_otp` factory supervisor with `Temporary` restart semantics to avoid replaying side-effecting requests.
- Keep the core package sans-IO: core builds requests and decodes gRPC messages; backends own network IO.
- Refactor unary `send` to collect from the streaming abstraction so there is only one transport path. Initial refactor is in place.
- Add cancellation via `close(stream)` that cancels the active gun stream and closes its connection.

## gRPC streaming

- Add an incremental gRPC frame decoder to core for chunked HTTP/2 data. Initial decoder and split-frame tests are in place.
- Add a backend `GrpcStream` wrapper that turns raw HTTP/2 data chunks into gRPC message frames. Initial wrapper and live append test are in place.
- Preserve raw stream access for debugging and lower-level operations.

## Streams API parity

- Implement bounded `read_stream` on top of the streaming backend. Initial stream read request and live backend read test are in place.
- Decode recorded events, resolved link events, positions, revisions, metadata, and content types. Initial recorded-event, resolved-link, and read bookkeeping decoders are in place; content type still needs explicit modelling.
- Implement `read_all` after `read_stream` works. Initial bounded request builder and backend `$all` read test are in place.
- Add `$all` filter support for stream-name and event-type predicates. Initial event-type and stream-name prefix/regex filter encoding is in place for read-all.
- Add link resolution decoding for reads and subscriptions. Initial `Recorded`/`Resolved` read-event modelling and decoder tests are in place.
- Model all read response variants: subscription confirmation, checkpoint, caught up, fell behind, first stream position, last stream position, and last all-stream position. Initial public variants and decoders are in place.
- Improve read stream-not-found handling in public APIs and backend helpers.
- Add `delete_stream` and `tombstone_stream` unary operations. Initial request builders, response decoders, and backend tests are in place.
- Add stream metadata helpers. Initial write/read request helpers are in place.
- Add `get_stream_metadata`. Initial helper reads the metadata stream and system metadata JSON decoding is in place.
- Add `set_stream_metadata`. Initial helper appends a `$metadata` JSON event to the metadata stream.
- Model stream ACLs. Initial ACL encoding and typed ACL decoding are in place.
- Model stream metadata fields such as max count, max age, truncate before, and cache control. Initial encoding and typed decoding are in place.

## Event model

- Model content type explicitly instead of relying only on raw metadata lookup.
- Add JSON decoding helpers for recorded events.
- Add binary recorded-event helpers.
- Add link event support. Initial resolved-link read event modelling is in place.
- Consider a dedicated all-stream recorded-event type if `$all` semantics need stronger typing.
- Decode created timestamps for recorded events.
- Support structured UUID decoding if non-string UUID mode is added.

## Long-lived subscriptions

- Implement `subscribe_to_stream` using the same streaming abstraction. Initial request builder, confirmation decoding, and live event delivery test are in place.
- Implement `subscribe_to_all` with filters and link resolution. Initial request builder, event-type filtered live test, and streaming backend support are in place.
- Add explicit cancellation and timeout behavior for subscription consumers.

## Persistent subscriptions

- Create persistent subscription to stream.
- Update persistent subscription to stream.
- Delete persistent subscription to stream.
- Create persistent subscription to `$all`.
- Update persistent subscription to `$all`.
- Delete persistent subscription to `$all`.
- Subscribe to persistent subscription to stream.
- Subscribe to persistent subscription to `$all`.
- Ack persistent subscription messages.
- Nack persistent subscription messages.
- Replay parked messages.
- List persistent subscriptions.
- Get persistent subscription info.
- Restart persistent subscription subsystem.

## Projections

- Create projection.
- Update projection.
- Delete projection.
- Enable projection.
- Disable projection.
- Reset projection.
- Get projection state.
- Get projection result.
- Get projection status.
- List projections.
- Restart projection subsystem.

## Operational parity

- Expand connection string parsing for credentials and connection options.
- Parse credentials from connection strings.
- Parse TLS verification options from connection strings.
- Parse CA certificate options from connection strings.
- Parse user certificate options from connection strings.
- Parse discovery options from connection strings.
- Parse node preference from connection strings.
- Parse deadlines and timeouts from connection strings.
- Parse connection name from connection strings.
- Add authorization headers.
- Add per-request credentials.
- Add credentials provider equivalent.
- Add `connection-name` headers.
- Add `requires-leader` headers.
- Improve error types for gRPC statuses such as access denied, not authenticated, not leader, unavailable, stream not found, stream deleted, deadline exceeded, and wrong expected version. Initial status-code mapping is in place for access denied, not authenticated, unavailable, stream not found, stream deleted, deadline exceeded, wrong expected version, and unknown statuses.
- Include wrong-expected-version details when KurrentDB returns them.
- Include not-leader endpoint details when KurrentDB returns them.

## Backend runtime

- Consider a shared HTTP/2 connection manager only after the one-connection-per-stream implementation is correct.
- Add higher-level collected read helpers if they remain useful after stream APIs settle.
- Add ergonomic cancellable subscription consumers.
- Add graceful shutdown for named stream supervisors.
- Add reconnection policy.
- Add retry policy.
- Add keepalive options.
- Add deadline options.
- Add TLS certificate options.

## Testing and docs

- Add more repeat-safe integration fixtures.
- Add tests for non-happy-path gRPC statuses. Initial core tests are in place for stream deleted, access denied, not authenticated, and unknown statuses.
- Add tests for stream deleted. Initial core decoder test is in place.
- Add tests for stream not found.
- Add tests for wrong expected revision.
- Add tests for split HTTP/2 chunks with the live backend if feasible.
- Update README examples to match the current API.
- Add examples for append, read stream, read all, delete, and tombstone.

## Tradeoffs recorded while building

- Read responses now expose subscription confirmation, checkpoint, caught-up, fell-behind, first/last stream position, and last `$all` position. Timestamp fields on checkpoint/caught-up/fell-behind are intentionally not decoded yet; the current API focuses on resumability state first.
- Public read revision variants use `FromStart`, `FromEnd`, and `FromRevision` instead of generic `Start`/`End` to avoid future module-level variant name collisions.
- Backend tests use float-based unique stream names to avoid persisted local state across repeated runs. This is pragmatic test-only uniqueness, not a production ID strategy.
- Delete and tombstone have separate option types even though they currently only contain `expected_revision`; this leaves room for operation-specific options without reusing append-specific configuration.
- `read_all` reuses `ReadMessage` and `RecordedEvent` rather than introducing separate `$all` event types. This keeps bounded reads compact; richer all-stream-specific modelling can be added with filters/subscriptions.
- The `$all` integration test now uses event-type prefix filtering, but still consumes a bounded stream manually. A higher-level collection helper can simplify this later.
- gRPC status mapping is code-first and message-text-assisted where KurrentDB does not return structured details, such as stream deleted. For these cases the full server message is stored as context rather than parsing a stream name out of human text.
- `GrpcStatus(String)` remains in the public error type temporarily, but known non-zero gRPC statuses now map to structured domain variants and unknown statuses map to `UnknownGrpcStatus(status, message)`.
- `set_stream_metadata` requires the caller to supply the metadata event UUID because the sans-IO core does not generate UUIDs. Runtime backends can add ergonomic UUID-generating helpers later.
- `get_stream_metadata` currently returns a read request for the metadata stream and relies on the existing recorded-event decoder. System metadata JSON can be decoded with `decode_stream_metadata`, but arbitrary custom JSON fields are not preserved yet pending a deliberate public JSON value representation.
- Read-all filters support event-type and stream-name prefix/regex plus max/count windows. Checkpoint interval multiplier is left at the server default until subscriptions/checkpoints need explicit control.
- `subscribe_to_stream` reuses `ReadMessage` for subscription confirmations and events because KurrentDB uses the same `ReadResp` stream. Explicit cancellation ergonomics and subscription-specific wrapper types are still deferred.
- `subscribe_to_all` uses a subscription-specific filter encoder that sets `checkpointIntervalMultiplier` to `1`. Bounded `read_all` keeps the server default because it already works without explicit checkpoint configuration, while filtered `$all` subscriptions closed immediately without it.
- `ReadMessage.ReadEvent` now carries `Recorded(event)` or `Resolved(link, event)` instead of a raw `RecordedEvent`. This is a deliberate breaking change before publication so resolved-link semantics are not hidden from users.
- Unary public operations now accept an injected `send` function and decode responses internally. This keeps the core sans-IO while avoiding a separate public request-builder API for append, delete, tombstone, and set-stream-metadata. Streaming reads/subscriptions still expose requests because they need a receive loop; the next ergonomics pass should apply the same transport-injection idea to streaming with a richer transport shape.
