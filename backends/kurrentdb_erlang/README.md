# kurrentdb_erlang

Erlang HTTP/2 transport for the sans-IO `kurrentdb` package.

```gleam
import gleam/json
import kurrentdb
import kurrentdb_erlang

pub fn main() -> Nil {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113?tls=false")
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000000",
      event_type: "order-created",
      data: json.object([#("orderId", json.string("order-1"))]),
    )
  let assert Ok(request) =
    kurrentdb.append_to_stream(
      client,
      stream: "orders",
      events: [event],
      options: kurrentdb.default_append_options(),
    )

  let assert Ok(response) = kurrentdb_erlang.send(request)
}
```

## Development

```sh
gleam test  # Run the tests
```
