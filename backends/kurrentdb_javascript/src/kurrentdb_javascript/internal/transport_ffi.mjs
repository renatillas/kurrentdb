// Cross-runtime HTTP/2 + fetch transport for KurrentDB gRPC.
//
// Node.js: uses node:http2 for cleartext HTTP/2 (h2c).
// Bun:     uses native fetch (Bun supports h2c out of the box).
// Deno:    uses native fetch (HTTP/2 via TLS; h2c may not work).
//
// All public functions return Gleam-compatible Result types using the
// Gleam runtime's Result$Ok / Result$Error constructors.

import { Result$Ok, Result$Error, BitArray$BitArray, List } from "../../gleam.mjs";
import { Option$Some, Option$None } from "../../../gleam_stdlib/gleam/option.mjs";

let http2 = null;
let useHttp2 = false;

const isNode =
  typeof process !== "undefined" &&
  process.versions != null &&
  process.versions.node != null;


if (isNode) {
  try {
    http2 = await import("node:http2");
    useHttp2 = true;
  } catch { }
}

function gleamHeadersToRecord(headers) {
  const record = {};
  for (const [k, v] of headers) {
    record[k.toLowerCase()] = v;
  }
  return record;
}


function gleamBodyToBuffer(body) {
  if (body == null) return undefined;
  if (body instanceof Uint8Array) return body;
  let raw = body.rawBuffer;
  if (raw.byteLength > 0) return raw;
  return undefined;
}

function recordToGleamHeaders(record) {
  const headers = [];
  for (const [k, v] of Object.entries(record)) {
    if (!k.startsWith(":")) {
      headers.push([k, String(v)]);
    }
  }
  return List.fromArray(headers);
}

function errorMessage(err) {
  return err && typeof err.message === "string"
    ? err.message
    : String(err ?? "unknown error");
}

function buildUrl(host, path) {
  const urlStr = host.includes("://") ? host : `http://${host}`;
  const base = urlStr.endsWith("/") ? urlStr : urlStr + "/";
  return new URL(path.replace(/^\//, ""), base);
}

// ── Node.js HTTP/2 ─────────────────────────────────────────────────

async function nodeSend(method, host, path, gleamHeaders, body) {
  const url = buildUrl(host, path);
  const origin = `${url.protocol}//${url.host}`;
  const client = http2.connect(origin);

  try {
    const result = await new Promise((resolve, reject) => {
      const reqHeaders = {
        ":method": method,
        ":path": url.pathname + url.search,
        ...gleamHeadersToRecord(gleamHeaders),
      };
      const req = client.request(reqHeaders);
      const chunks = [];

      req.on("response", (respHeaders) => {
        req.on("data", (c) => chunks.push(c));
        req.on("end", () => {
          resolve({
            status: respHeaders[":status"] || 200,
            headers: recordToGleamHeaders(respHeaders),
            body: Buffer.concat(chunks),
          });
        });
      });
      req.on("error", reject);
      client.on("error", reject);
      req.end(gleamBodyToBuffer(body));
    });

    return Result$Ok([result.status, result.headers, BitArray$BitArray(result.body)]);
  } catch (err) {
    return Result$Error(errorMessage(err));
  } finally {
    client.close();
  }
}

async function nodeOpenStream(method, host, path, gleamHeaders, body) {
  const url = buildUrl(host, path);
  const origin = `${url.protocol}//${url.host}`;
  const client = http2.connect(origin);

  try {
    const result = await new Promise((resolve, reject) => {
      const reqHeaders = {
        ":method": method,
        ":path": url.pathname + url.search,
        ...gleamHeadersToRecord(gleamHeaders),
      };
      const req = client.request(reqHeaders);
      let resolvedHeaders = null;
      const chunks = [];
      let ended = false;

      req.on("response", (respHeaders) => {
        resolvedHeaders = respHeaders;
      });

      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        ended = true;
      });
      req.on("close", () => {
        client.close();
      });
      req.on("error", reject);

      req.end(gleamBodyToBuffer(body));

      // Wait for headers (they arrive asynchronously after first data)
      const poll = () => {
        if (resolvedHeaders) {
          const reader = {
            _buffer: chunks,
            _ended: ended,
            _index: 0,
            _resolves: [],
            _done: false,
            read() {
              if (this._index < this._buffer.length) {
                const value = this._buffer[this._index++];
                return Promise.resolve({
                  done: false,
                  value: Uint8Array.prototype.isPrototypeOf(value)
                    ? value
                    : new Uint8Array(value),
                });
              }
              if (this._ended && this._index >= this._buffer.length) {
                return Promise.resolve({ done: true, value: undefined });
              }
              // Wait for more data
              return new Promise((resolve) => {
                this._resolves.push(resolve);
              });
            },
            _push(chunk) {
              this._buffer.push(chunk);
              if (this._resolves.length > 0) {
                const resolve = this._resolves.shift();
                resolve({
                  done: false,
                  value: Uint8Array.prototype.isPrototypeOf(chunk)
                    ? chunk
                    : new Uint8Array(chunk),
                });
              }
            },
            _finish() {
              this._ended = true;
              if (this._resolves.length > 0) {
                for (const resolve of this._resolves) {
                  resolve({ done: true, value: undefined });
                }
              }
            },
          };

          // Patch req to push data through our reader
          req.removeAllListeners("data");
          req.removeAllListeners("end");
          req.on("data", (c) => reader._push(c));
          req.on("end", () => reader._finish());

          resolve({
            status: resolvedHeaders[":status"] || 200,
            headers: recordToGleamHeaders(resolvedHeaders),
            reader,
          });
        } else {
          setImmediate(poll);
        }
      };
      poll();
    });

    return Result$Ok([result.status, result.headers, result.reader]);
  } catch (err) {
    return Result$Error(errorMessage(err));
  }
}

// ── fetch fallback (Bun, Deno) ─────────────────────────────────────────

async function fetchSend(method, host, path, gleamHeaders, body) {
  try {
    const url = buildUrl(host, path).href;
    const headerRecord = gleamHeadersToRecord(gleamHeaders);
    const response = await fetch(url, {
      method,
      headers: headerRecord,
      body: gleamBodyToBuffer(body) ?? null,
    });

    const headers = [];
    response.headers.forEach((v, k) => headers.push([k, v]));

    const buf = new Uint8Array(await response.arrayBuffer());
    return Result$Ok([response.status, List.fromArray(headers), BitArray$BitArray(buf)]);
  } catch (err) {
    return Result$Error(errorMessage(err));
  }
}

async function fetchOpenStream(method, host, path, gleamHeaders, body) {
  try {
    const url = buildUrl(host, path).href;
    const headerRecord = gleamHeadersToRecord(gleamHeaders);
    const response = await fetch(url, {
      method,
      headers: headerRecord,
      body: gleamBodyToBuffer(body) ?? null,
    });

    const headers = [];
    response.headers.forEach((v, k) => headers.push([k, v]));

    const nativeReader = response.body.getReader();
    const reader = {
      async read() {
        return nativeReader.read().then(({ done, value }) => ({
          done,
          value: value ? new Uint8Array(value) : undefined,
        }));
      },
    };

    return Result$Ok([response.status, List.fromArray(headers), reader]);
  } catch (err) {
    return Result$Error(errorMessage(err));
  }
}

// ── Public API ────────────────────────────────────────────────────────

export function send_request(method, host, path, headers, body) {
  if (useHttp2) return nodeSend(method, host, path, headers, body);
  return fetchSend(method, host, path, headers, body);
}

export function open_stream(method, host, path, headers, body) {
  if (useHttp2) return nodeOpenStream(method, host, path, headers, body);
  return fetchOpenStream(method, host, path, headers, body);
}

export async function read_chunk(reader) {
  try {
    const { done, value } = await reader.read();
    if (done) return Result$Ok(Option$None());
    if (value && value.byteLength > 0) {
      return Result$Ok(Option$Some(BitArray$BitArray(value)));
    }
    // Zero-length chunk: try reading next
    return await read_chunk(reader);
  } catch (err) {
    console.error("read_chunk error:", err, err?.stack);
    return Result$Error(errorMessage(err));
  }
}
