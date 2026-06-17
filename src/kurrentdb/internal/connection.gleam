import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/uri

pub type Error {
  InvalidUri
  InvalidScheme(scheme: String)
  MissingHost
  InvalidTls(value: String)
}

pub type Config {
  Config(endpoint: String, tls: Bool)
}

pub fn parse(connection_string: String) -> Result(Config, Error) {
  use uri <- result.try(
    uri.parse(connection_string)
    |> result.map_error(fn(_) { InvalidUri }),
  )
  use Nil <- result.try(validate_scheme(uri.scheme))
  use host <- result.try(case uri.host {
    Some(host) -> Ok(host)
    None -> Error(MissingHost)
  })
  use tls <- result.try(parse_tls(uri.query))

  let scheme = case tls {
    True -> "https"
    False -> "http"
  }
  let port = case uri.port {
    Some(port) -> ":" <> int.to_string(port)
    None -> ""
  }

  Ok(Config(endpoint: scheme <> "://" <> host <> port, tls: tls))
}

fn validate_scheme(scheme: Option(String)) -> Result(Nil, Error) {
  case scheme {
    Some("kurrentdb") | Some("esdb") -> Ok(Nil)
    Some(scheme) -> Error(InvalidScheme(scheme))
    None -> Error(InvalidUri)
  }
}

fn parse_tls(query: Option(String)) -> Result(Bool, Error) {
  case query {
    None -> Ok(True)
    Some(query) -> {
      use pairs <- result.try(
        uri.parse_query(query)
        |> result.replace_error(InvalidUri),
      )
      case list.key_find(pairs, "tls") {
        Error(Nil) -> Ok(True)
        Ok("true") -> Ok(True)
        Ok("false") -> Ok(False)
        Ok(value) -> Error(InvalidTls(value))
      }
    }
  }
}
