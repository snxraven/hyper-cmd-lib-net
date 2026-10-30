const dgram = require('dgram')

const DEFAULT_MAX_FRAME_SIZE = 65535

// Datagrams are UDX unreliable messages (secret-stream trySend / 'message'),
// not length-prefixed bytes on the reliable stream. A lost reliable packet
// retransmits and stalls everything queued behind it; a game datagram must not.

function sendDatagram(stream, msg) {
  if (typeof stream.trySend === 'function') {
    stream.trySend(msg)
    return
  }
  if (typeof stream.send === 'function') {
    const result = stream.send(msg)
    if (result && typeof result.catch === 'function') result.catch(noop)
    return
  }
  stream.destroy(new Error('UDP tunnel stream has no unreliable send'))
}

// Noise secret-streams drop trySend until the handshake sets `connected`.
// Streams that never set `connected` (plain test doubles) are ready immediately.
function createDatagramSender(stream) {
  let ready = stream.connected !== false
  let closed = false
  const queue = []

  const flush = () => {
    while (queue.length) sendDatagram(stream, queue.shift())
  }

  const onReady = () => {
    if (closed || ready) return
    ready = true
    stream.removeListener('connect', onReady)
    flush()
  }

  if (!ready) stream.on('connect', onReady)

  return {
    send(msg) {
      if (closed) return
      if (!ready) {
        queue.push(Buffer.from(msg))
        return
      }
      sendDatagram(stream, msg)
    },
    close() {
      if (closed) return
      closed = true
      queue.length = 0
      stream.removeListener('connect', onReady)
    }
  }
}

// Attach before the tunnel header has been parsed. Unreliable messages are not
// ordered with the reliable stream, so the first datagrams can arrive first
// and would otherwise be dropped.
function bufferUdpMessages(stream) {
  const pending = []
  const onMessage = (msg) => {
    pending.push(Buffer.from(msg))
  }
  stream.on('message', onMessage)
  return {
    pending,
    stop() {
      stream.removeListener('message', onMessage)
    }
  }
}

function createUdpFramedProxy(createTunnel, opts = {}, onBind) {
  const port = opts.port
  const host = opts.host
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }
  const maxFrameSize = opts.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE

  const proxySocket = dgram.createSocket('udp4')
  const clients = new Map()

  proxySocket.on('error', (err) => {
    logger.error(`Proxy socket error: ${err.stack}`)
    proxySocket.close()
  })

  proxySocket.on('message', (msg, rinfo) => {
    const clientId = `${rinfo.address}:${rinfo.port}`
    logger.debug(`UDP message from ${clientId}: ${msg.length} bytes`)
    let client = clients.get(clientId)
    if (!client) {
      const stream = createTunnel()
      const sender = createDatagramSender(stream)
      client = { stream, rinfo, sender }
      clients.set(clientId, client)
      stream.on('message', (response) => {
        if (response.length > maxFrameSize) {
          logger.error(`Datagram too large from ${clientId}: ${response.length} > ${maxFrameSize}`)
          clients.delete(clientId)
          sender.close()
          stream.destroy(new Error(`Datagram too large: ${response.length}`))
          return
        }
        logger.debug(`UDP response for ${clientId}: ${response.length} bytes`)
        const dest = client.rinfo
        proxySocket.send(response, 0, response.length, dest.port, dest.address, (err) => {
          if (err) logger.error(`Socket error while sending packet to ${clientId}: ${err.stack}`)
        })
      })
      stream.on('error', (err) => {
        logger.error(`Remote error for ${clientId}: ${err.stack}`)
        sender.close()
        clients.delete(clientId)
        stream.destroy()
      })
      stream.on('close', () => {
        logger.debug(`Remote close for ${clientId}`)
        sender.close()
        clients.delete(clientId)
      })
    }
    client.rinfo = rinfo
    client.sender.send(msg)
  })

  proxySocket.bind(port, host, onBind)

  return { proxySocket, clients }
}

function pipeUdpFramedServer(stream, leftover, opts = {}) {
  const port = opts.port
  const host = opts.host
  const logger = opts.logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  }
  const maxFrameSize = opts.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE
  const pending = opts.pending

  const localSocket = dgram.createSocket('udp4')
  const sender = createDatagramSender(stream)

  let destroyed = false
  const cleanup = (err) => {
    if (destroyed) return
    destroyed = true
    sender.close()
    if (err) logger.info(`Destroying pipe due to error: ${err.message}`)
    else logger.info('Destroying pipe')
    try {
      localSocket.close()
    } catch {}
    if (err) stream.destroy(err)
    else stream.end()
  }

  const forwardToLocal = (msg) => {
    if (destroyed) return
    if (msg.length > maxFrameSize) {
      cleanup(new Error(`Datagram too large: ${msg.length} > ${maxFrameSize}`))
      return
    }
    localSocket.send(msg, 0, msg.length, port, host, (err) => {
      if (err) logger.error(`Error sending packet to local socket: ${err.stack}`)
    })
  }

  localSocket.on('error', (err) => {
    logger.error(`Local UDP socket error: ${err.stack}`)
    cleanup(err)
  })
  localSocket.on('message', (msg) => {
    logger.debug(`Data from local to remote: ${msg.length} bytes`)
    sender.send(msg)
  })
  localSocket.on('close', () => cleanup())
  stream.on('message', (msg) => {
    logger.debug(`Data from remote to local: ${msg.length} bytes`)
    forwardToLocal(msg)
  })
  stream.on('end', () => cleanup())
  stream.on('close', () => cleanup())
  stream.on('error', (err) => cleanup(err))

  if (pending && pending.length) {
    for (const msg of pending) {
      if (destroyed) break
      forwardToLocal(msg)
    }
  }
  if (!destroyed && leftover && leftover.length) forwardToLocal(leftover)
}

function noop() {}

module.exports = {
  createUdpFramedProxy,
  pipeUdpFramedServer,
  bufferUdpMessages
}
