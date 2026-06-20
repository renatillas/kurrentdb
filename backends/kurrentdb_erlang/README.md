# kurrentdb_erlang

Erlang HTTP/2 transport for the sans-IO `kurrentdb` package.

```gleam
import gleam/option
import gleam/json
import kurrentdb
import kurrentdb_erlang

pub fn main() -> Nil {
  let assert Ok(client) =
    kurrentdb.from_connection_string("kurrentdb://localhost:2113?tls=true")
  let assert Ok(connection) = kurrentdb_erlang.start(option.None)

  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000000",
      event_type: "order-created",
      data: json.object([#("orderId", json.string("order-1"))]),
    )
  let assert Ok(_) =
    kurrentdb.append_to_stream(
      client,
      stream: "orders",
      events: [event],
      options: kurrentdb.default_append_options(),
      using: kurrentdb_erlang.send(connection),
    )
}
```

## Development

```sh
./scripts/generate-dev-certs.sh
docker compose -f docker-compose.yml up -d
gleam test
```

Hackney uses TLS HTTP/2. That's why we need the certs
