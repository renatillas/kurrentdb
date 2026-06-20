import gleam/http/request
import kurrentdb
import kurrentdb/operation/read_all

pub opaque type Configuration {
  Builder(
    direction: read_all.Direction,
    from_position: read_all.Position,
    resolve_links: Bool,
    filter: read_all.Filter,
  )
}

pub fn configure() -> Configuration {
  Builder(
    direction: read_all.Forwards,
    from_position: read_all.FromEnd,
    resolve_links: False,
    filter: read_all.NoFilter,
  )
}

pub fn direction(
  config: Configuration,
  direction: read_all.Direction,
) -> Configuration {
  Builder(..config, direction: direction)
}

pub fn from_position(
  config: Configuration,
  position: read_all.Position,
) -> Configuration {
  Builder(..config, from_position: position)
}

pub fn resolve_links(
  config: Configuration,
  resolve_links: Bool,
) -> Configuration {
  Builder(..config, resolve_links: resolve_links)
}

pub fn filter(config: Configuration, filter: read_all.Filter) -> Configuration {
  Builder(..config, filter: filter)
}

pub fn request(
  client: kurrentdb.Client,
  config config: Configuration,
) -> request.Request(BitArray) {
  let message =
    read_all.encode_subscribe_request(
      direction: config.direction,
      from_position: config.from_position,
      resolve_links: config.resolve_links,
      filter: config.filter,
    )

  kurrentdb.grpc_request(client, read_all.path(), [message])
}
