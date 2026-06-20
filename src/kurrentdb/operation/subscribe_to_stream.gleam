import gleam/http/request
import kurrentdb
import kurrentdb/operation/read_stream

pub opaque type Configuration {
  Builder(
    direction: read_stream.Direction,
    from_revision: read_stream.ReadRevision,
    resolve_links: Bool,
  )
}

pub fn configure() -> Configuration {
  Builder(
    direction: read_stream.Forwards,
    from_revision: read_stream.FromEnd,
    resolve_links: False,
  )
}

pub fn direction(
  config: Configuration,
  direction: read_stream.Direction,
) -> Configuration {
  Builder(..config, direction: direction)
}

pub fn from_revision(
  config: Configuration,
  revision: read_stream.ReadRevision,
) -> Configuration {
  Builder(..config, from_revision: revision)
}

pub fn resolve_links(
  config: Configuration,
  resolve_links: Bool,
) -> Configuration {
  Builder(..config, resolve_links: resolve_links)
}

pub fn request(
  client: kurrentdb.Client,
  stream stream_name: String,
  config config: Configuration,
) -> request.Request(BitArray) {
  let message =
    read_stream.encode_subscribe_request(
      stream: stream_name,
      direction: config.direction,
      from_revision: config.from_revision,
      resolve_links: config.resolve_links,
    )

  kurrentdb.grpc_request(client, read_stream.path(), [message])
}
