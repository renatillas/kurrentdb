# kurrentdb

A Gleam client foundation for KurrentDB.

```gleam
import kurrentdb
import gleam/json

pub fn main() -> Nil {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113?tls=false")

  let event = kurrentdb.json_event(
    "order-created",
    json.object([#("orderId", json.string("order-1"))]),
  )

  let assert Ok(append_request) =
    kurrentdb.append_to_stream(
      client,
      "orders",
      [event],
      kurrentdb.append_options()
      |> kurrentdb.expected_revision(kurrentdb.NoStream),
    )

  // Sans-IO: hand this HTTP/2 gRPC request to a transport implementation.
  let http_request = kurrentdb.http_request(append_request)

  // After the transport receives a response:
  // let result = kurrentdb.decode(append_request, response)
}
```

## Status

This is an early implementation. It currently includes protobuf codecs for
append-to-stream and a sans-io transport boundary. The core package builds typed
requests and decodes responses; separate transport packages perform I/O.

## Development

```sh
gleam test  # Run the tests
sh scripts/integration.sh  # Start KurrentDB with Docker Compose and run tests
```

Set `KEEP_KURRENTDB=1` to leave the local container running after integration
tests complete.
