const { Duplex } = require('streamx')
const net = require('net')
const dgram = require('dgram')

function fakeSocket() {
  const written = []
  const messages = []
  const s = new Duplex({
    read() {},
    write(chunk, cb) {
      written.push(chunk)
      cb(null)
    }
  })
  s.written = written
  s.messages = messages
  s.trySend = (buf) => {
    messages.push(Buffer.from(buf))
  }
  return s
}

// A stream that echoes back whatever is written to it, simulating a remote
// peer that reflects data (used as the "far end" of a tunnel/proxy).
function echoStream() {
  const s = new Duplex({
    read() {},
    write(chunk, cb) {
      this.push(chunk)
      cb(null)
    }
  })
  s.trySend = (buf) => {
    s.emit('message', Buffer.from(buf))
  }
  return s
}

function createLogger() {
  const calls = { debug: [], info: [], warn: [], error: [] }
  const logger = {
    debug: (...a) => calls.debug.push(a),
    info: (...a) => calls.info.push(a),
    warn: (...a) => calls.warn.push(a),
    error: (...a) => calls.error.push(a)
  }
  return { logger, calls }
}

function tick(ms = 20) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function bindUdpClient() {
  return new Promise((resolve, reject) => {
    const sock = dgram.createSocket('udp4')
    sock.on('error', reject)
    sock.bind(0, '127.0.0.1', () => resolve(sock))
  })
}

function listenTcp(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address()))
  })
}

// Two real, independently connected loopback TCP sockets. `local` is the
// server-accepted side — what gets handed to connPiper as `a` or `b` — and
// `remote` is the test's own hook into "the other end" of that connection,
// used to inject inbound bytes (remote.write) and observe whatever connPiper
// forwarded out to it (remote.on('data', ...)).
function tcpPair() {
  return new Promise((resolve) => {
    const server = net.createServer({ allowHalfOpen: true })
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      const remote = net.connect(addr.port, '127.0.0.1')
      server.once('connection', (local) => resolve({ local, remote, server }))
    })
  })
}

function closePair(pair) {
  pair.remote.destroy()
  pair.local.destroy()
  pair.server.close()
}

function readOnce(sock) {
  return new Promise((resolve) => sock.once('data', (d) => resolve(d.toString())))
}

async function withEchoUdpServer(fn) {
  const server = dgram.createSocket('udp4')
  server.on('message', (msg, rinfo) => {
    server.send(msg, 0, msg.length, rinfo.port, rinfo.address)
  })
  await new Promise((resolve) => server.bind(0, '127.0.0.1', resolve))
  try {
    await fn(server.address())
  } finally {
    server.close()
  }
}

module.exports = {
  fakeSocket,
  echoStream,
  createLogger,
  tick,
  bindUdpClient,
  listenTcp,
  tcpPair,
  closePair,
  readOnce,
  withEchoUdpServer
}
