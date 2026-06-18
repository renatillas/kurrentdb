# kurrentdb

A Gleam client foundation for KurrentDB.

```gleam
import kurrentdb
import kurrentdb_erlang

import gleam/json

pub fn main() -> Nil {
  let name = process.new_name("kurrentdb")
  let assert Ok(_supervisor) = kurrentdb_erlang.start_stream(name)

  // Create the client
  let assert Ok(client) = kurrentdb.from_connection_string(connection_string)

  // Create the event
  let event =
    kurrentdb.json_event(
      uuid: "00000000-0000-4000-8000-000000000000",
      event_type: "kurrentdb-erlang-test",
      data: json.object([#("runtime", json.string("erlang"))]),
    )

  // Append to the stream
  let assert Ok(kurrentdb.AppendSuccess(current_revision: 0, position: _)) =
    kurrentdb.append_to_stream(
      client,
      stream: "kurrentdb-erlang-integration",
      events: [event],
      options: kurrentdb.default_append_options()
        |> kurrentdb.expected_revision(kurrentdb.Any),
      // Using a sans-io we can attach a independant backend to do the request for us.
      using: kurrentdb_erlang.send(name, _),
    )
}
```

## Status

This is an early implementation. It currently includes protobuf codecs and a sans-io transport boundary. The core package builds typed
requests and decodes responses; separate transport packages perform I/O.

## Development

```sh
gleam test  # Run the tests
```
