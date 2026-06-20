import gleam/http/request
import gleam/list
import kurrentdb

const streams_service: String = "event_store.client.streams.Streams"

const read_method: String = "Read"

pub opaque type Configuration {
  Builder(
    direction: Direction,
    from_position: Position,
    max_count: Int,
    resolve_links: Bool,
    filter: Filter,
  )
}

pub type Direction {
  Forwards
  Backwards
}

pub type Position {
  FromStart
  FromEnd
  FromPosition(commit_position: Int, prepare_position: Int)
}

pub type Filter {
  NoFilter
  EventTypePrefix(prefixes: List(String), window: FilterWindow)
  EventTypeRegex(regex: String, window: FilterWindow)
  StreamNamePrefix(prefixes: List(String), window: FilterWindow)
  StreamNameRegex(regex: String, window: FilterWindow)
}

pub type FilterWindow {
  FilterMax(Int)
  FilterCount
}

pub fn configure() -> Configuration {
  Builder(
    direction: Forwards,
    from_position: FromStart,
    max_count: 1000,
    resolve_links: False,
    filter: NoFilter,
  )
}

pub fn direction(config: Configuration, direction: Direction) -> Configuration {
  Builder(..config, direction: direction)
}

pub fn position(config: Configuration, position: Position) -> Configuration {
  Builder(..config, from_position: position)
}

pub fn max_count(config: Configuration, max_count: Int) -> Configuration {
  Builder(..config, max_count: max_count)
}

pub fn resolve_links(
  config: Configuration,
  resolve_links: Bool,
) -> Configuration {
  Builder(..config, resolve_links: resolve_links)
}

pub fn filter(config: Configuration, filter: Filter) -> Configuration {
  Builder(..config, filter: filter)
}

pub fn request(
  client: kurrentdb.Client,
  config config: Configuration,
) -> request.Request(BitArray) {
  let message =
    encode_read_all_request(
      config.direction,
      config.from_position,
      config.max_count,
      config.resolve_links,
      config.filter,
    )

  kurrentdb.grpc_request(client, "/" <> streams_service <> "/" <> read_method, [
    message,
  ])
}

@internal
pub fn encode_subscribe_request(
  direction direction: Direction,
  from_position from_position: Position,
  resolve_links resolve_links: Bool,
  filter filter: Filter,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(2, encode_position(from_position)),
        kurrentdb.encode_int32_field(3, direction_to_int(direction)),
        kurrentdb.encode_int32_field(4, bool_to_int(resolve_links)),
        kurrentdb.encode_message_field(6, kurrentdb.encode_empty()),
        encode_subscribe_filter(filter),
        kurrentdb.encode_message_field(
          9,
          kurrentdb.encode_message([
            kurrentdb.encode_message_field(2, kurrentdb.encode_empty()),
          ]),
        ),
      ]),
    ),
  ])
}

@internal
pub fn path() -> String {
  "/" <> streams_service <> "/" <> read_method
}

fn encode_read_all_request(
  direction: Direction,
  from_position: Position,
  max_count: Int,
  resolve_links: Bool,
  filter: Filter,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(
      1,
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(2, encode_position(from_position)),
        kurrentdb.encode_int32_field(3, direction_to_int(direction)),
        kurrentdb.encode_int32_field(4, bool_to_int(resolve_links)),
        kurrentdb.encode_uint64_field(5, max_count),
        encode_filter(filter),
        kurrentdb.encode_message_field(
          9,
          kurrentdb.encode_message([
            kurrentdb.encode_message_field(2, kurrentdb.encode_empty()),
          ]),
        ),
      ]),
    ),
  ])
}

fn encode_position(from_position: Position) -> BitArray {
  case from_position {
    FromPosition(commit_position, prepare_position) ->
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(
          1,
          kurrentdb.encode_position(kurrentdb.Position(
            commit_position:,
            prepare_position:,
          )),
        ),
      ])
    FromStart ->
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(2, kurrentdb.encode_empty()),
      ])
    FromEnd ->
      kurrentdb.encode_message([
        kurrentdb.encode_message_field(3, kurrentdb.encode_empty()),
      ])
  }
}

fn encode_subscribe_filter(filter: Filter) -> BitArray {
  case filter {
    NoFilter -> kurrentdb.encode_message_field(8, kurrentdb.encode_empty())
    EventTypePrefix(prefixes, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          2,
          encode_filter_prefixes(prefixes),
          window,
          1,
        ),
      )
    EventTypeRegex(regex, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          2,
          kurrentdb.encode_string_field(1, regex),
          window,
          1,
        ),
      )
    StreamNamePrefix(prefixes, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          1,
          encode_filter_prefixes(prefixes),
          window,
          1,
        ),
      )
    StreamNameRegex(regex, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options_with_checkpoint(
          1,
          kurrentdb.encode_string_field(1, regex),
          window,
          1,
        ),
      )
  }
}

fn encode_filter(filter: Filter) -> BitArray {
  case filter {
    NoFilter -> kurrentdb.encode_message_field(8, kurrentdb.encode_empty())
    EventTypePrefix(prefixes, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options(2, encode_filter_prefixes(prefixes), window),
      )
    EventTypeRegex(regex, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options(
          2,
          kurrentdb.encode_string_field(1, regex),
          window,
        ),
      )
    StreamNamePrefix(prefixes, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options(1, encode_filter_prefixes(prefixes), window),
      )
    StreamNameRegex(regex, window) ->
      kurrentdb.encode_message_field(
        7,
        encode_filter_options(
          1,
          kurrentdb.encode_string_field(1, regex),
          window,
        ),
      )
  }
}

fn encode_filter_options(
  filter_field: Int,
  expression: BitArray,
  window: FilterWindow,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(filter_field, expression),
    encode_filter_window(window),
  ])
}

fn encode_filter_options_with_checkpoint(
  filter_field: Int,
  expression: BitArray,
  window: FilterWindow,
  checkpoint_interval_multiplier: Int,
) -> BitArray {
  kurrentdb.encode_message([
    kurrentdb.encode_message_field(filter_field, expression),
    encode_filter_window(window),
    kurrentdb.encode_int32_field(5, checkpoint_interval_multiplier),
  ])
}

fn encode_filter_prefixes(prefixes: List(String)) -> BitArray {
  kurrentdb.encode_message(
    list.map(prefixes, kurrentdb.encode_string_field(2, _)),
  )
}

fn encode_filter_window(window: FilterWindow) -> BitArray {
  case window {
    FilterMax(max) -> kurrentdb.encode_int32_field(3, max)
    FilterCount -> kurrentdb.encode_message_field(4, kurrentdb.encode_empty())
  }
}

fn direction_to_int(direction: Direction) -> Int {
  case direction {
    Forwards -> 0
    Backwards -> 1
  }
}

fn bool_to_int(value: Bool) -> Int {
  case value {
    True -> 1
    False -> 0
  }
}
