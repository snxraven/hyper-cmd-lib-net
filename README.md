# hyper-cmd-lib-net

Pipe local and remote (hyperdht) TCP/UDP streams together.

```
npm i @holesail/hyper-cmd-lib-net
```

## Usage

```js
const { createTcpProxy } = require('@holesail/hyper-cmd-lib-net')

const proxy = createTcpProxy(
  () => tunnel.createStream(),
  { port: 8080, host: '127.0.0.1' },
  () => console.log('listening on', proxy.address())
)
```

## API

#### `connPiper(a, bFactory, [opts])`

Pipe two duplex streams together, and destroy both the moment either one closes or
errors.

`bFactory()` is called once to make the second stream. Throw, or return `null`, to
reject the connection — `a` gets destroyed and `bFactory` is never called again.

```js
connPiper(a, () => b, {
  logger: null, // { debug, info, warn, error }, defaults to a no-op logger
  onDestroy: (err) => {} // called once, when the pipe is torn down
})
```

#### `const proxy = createTcpProxy(remoteTunnel, opts, onListen)`

Start a TCP server. Every accepted connection is piped to a fresh `remoteTunnel()`
stream using `connPiper` above.

```js
createTcpProxy(
  () => tunnel.createStream(),
  {
    port: 0,
    host: '127.0.0.1',
    logger: null,
    onDestroy: (err) => {}
  },
  () => {}
)
```

Returns the underlying `net.Server`. `proxy.close()` also destroys any connections
still open, so it doesn't need `'close'` listeners on every socket to fully shut down.

#### `pipeTcpServer(remoteStream, leftover, opts)`

The other direction of `createTcpProxy` — connects out to a local TCP service and pipes
it to `remoteStream`.

```js
pipeTcpServer(remoteStream, leftoverBuffer, {
  port: 3000,
  host: '127.0.0.1'
})
```

`leftover` is a buffer of bytes already read off `remoteStream` (eg while sniffing a
protocol) that gets written to the local socket before piping starts. Pass `null` if
there isn't any.

#### UDP datagrams

UDP datagrams are independent and loss-tolerant. These helpers carry each one as an
unreliable message on the tunnel (`stream.trySend` / `stream.send` out, the `'message'`
event back) instead of writing length-prefixed bytes onto the reliable stream. The
reliable stream retransmits and congestion-controls, so one dropped packet would stall
every datagram queued behind it. A HyperDHT secret stream already encrypts `trySend`.

Datagrams are held until `stream.connected` becomes true (the noise handshake). A stream
that never sets `connected` is treated as ready.

#### `const capture = bufferUdpMessages(stream)`

Start copying `'message'` payloads off `stream` immediately. Unreliable messages are not
ordered with the reliable header, so they can arrive before that header has been parsed.
Call `capture.stop()` once `pipeUdpFramedServer` is about to take over, and pass
`capture.pending` as `opts.pending`.

#### `const { proxySocket, clients } = createUdpFramedProxy(createTunnel, opts, onBind)`

Bind a UDP socket. Every distinct sender (by `address:port`) gets its own tunnel
stream, made lazily via `createTunnel()` on its first packet.

```js
createUdpFramedProxy(
  () => tunnel.createStream(),
  {
    port: 0,
    host: '127.0.0.1',
    maxFrameSize: 65535,
    logger: null
  },
  () => {}
)
```

`clients` is a `Map` of `address:port` -> `{ stream, rinfo, sender }`. A datagram bigger
than `maxFrameSize` destroys that client's tunnel and evicts it — the next packet from
the same client just starts a new one.

#### `pipeUdpFramedServer(stream, leftover, opts)`

The other end of the pair. Forwards each `'message'` as a plain UDP datagram to a local
service, and sends replies back with `trySend`.

```js
pipeUdpFramedServer(remoteStream, leftoverBuffer, {
  port: 53,
  host: '127.0.0.1',
  maxFrameSize: 65535,
  pending: [] // datagrams captured with bufferUdpMessages before this call
})
```

`leftover` is bytes already read off the reliable stream with the tunnel header. When
non-empty it is forwarded as one datagram. A clean `end` on `stream` ends the local
socket. An `error`, or a datagram over `maxFrameSize`, destroys it.

## License

Apache-2.0
