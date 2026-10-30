const test = require('brittle')
const { pipeUdpFramedServer } = require('../lib/udp-framed.js')
const { fakeSocket, createLogger, tick, withEchoUdpServer } = require('./helpers.js')

test('pipeUdpFramedServer - forwards a datagram to the local service and sends the reply back', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })

    const payload = Buffer.from('ping-udp-server')
    stream.emit('message', payload)
    await tick(40)

    t.is(stream.written.length, 0, 'reply is not written onto the reliable stream')
    t.is(stream.messages.length, 1)
    t.alike(stream.messages[0], payload)

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - forwards leftover bytes as one datagram', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    const payload = Buffer.from('leftover-complete')
    pipeUdpFramedServer(stream, payload, { port: addr.port, host: '127.0.0.1' })
    await tick(40)

    t.alike(stream.messages[0], payload)

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - delivers leftover and a later message as independent datagrams', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, Buffer.from('from-leftover'), {
      port: addr.port,
      host: '127.0.0.1'
    })
    stream.emit('message', Buffer.from('from-message'))
    await tick(40)

    const payloads = stream.messages.map((buf) => buf.toString()).sort()
    t.alike(payloads, ['from-leftover', 'from-message'])

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - handles back-to-back messages', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })

    stream.emit('message', Buffer.from('one'))
    stream.emit('message', Buffer.from('two'))
    await tick(40)

    const payloads = stream.messages.map((buf) => buf.toString()).sort()
    t.alike(payloads, ['one', 'two'])

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - replays datagrams captured before the pipe was attached', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    const pending = [Buffer.from('early'), Buffer.from('also-early')]
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1', pending })
    await tick(40)

    const payloads = stream.messages.map((buf) => buf.toString()).sort()
    t.alike(payloads, ['also-early', 'early'])

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - ignores bytes on the reliable stream', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })

    stream.push(Buffer.from('not-a-datagram'))
    await tick(30)
    t.is(stream.messages.length, 0)

    stream.emit('message', Buffer.from('real'))
    await tick(40)
    t.alike(stream.messages[0], Buffer.from('real'))

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - holds local replies until the tunnel connects', async function (t) {
  await withEchoUdpServer(async (addr) => {
    const stream = fakeSocket()
    stream.connected = false
    stream.sentEarly = 0
    const send = stream.trySend
    stream.trySend = (buf) => {
      if (stream.connected !== true) stream.sentEarly++
      send(buf)
    }

    pipeUdpFramedServer(stream, null, { port: addr.port, host: '127.0.0.1' })
    stream.emit('message', Buffer.from('ping'))
    await tick(40)

    t.is(stream.messages.length, 0)
    t.is(stream.sentEarly, 0)

    stream.connected = true
    stream.emit('connect')
    await tick(20)

    t.alike(stream.messages[0], Buffer.from('ping'))
    t.is(stream.sentEarly, 0)

    stream.emit('end')
    await tick()
  })
})

test('pipeUdpFramedServer - a datagram over maxFrameSize destroys the stream with a descriptive error', async function (t) {
  const stream = fakeSocket()
  let err
  stream.on('error', (e) => {
    err = e
  })

  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', maxFrameSize: 4 })
  stream.emit('message', Buffer.from('too-big-payload'))
  await tick(30)

  t.ok(stream.destroyed)
  t.ok(err && /Datagram too large: 15 > 4/.test(err.message))
})

test('pipeUdpFramedServer - a remote stream error destroys the pipe', async function (t) {
  const stream = fakeSocket()
  const { logger, calls } = createLogger()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', logger })

  const closed = new Promise((resolve) => stream.once('close', resolve))
  stream.emit('error', new Error('peer blew up'))
  await closed

  t.ok(stream.destroyed)
  t.ok(calls.info.some((args) => /Destroying pipe due to error: peer blew up/.test(args[0])))
})

test('pipeUdpFramedServer - ends its writable side (no error) when the remote stream ends', async function (t) {
  const stream = fakeSocket()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1' })

  const finished = new Promise((resolve) => stream.once('finish', resolve))
  stream.emit('end')
  await finished

  t.absent(stream.destroyed, 'a clean end() does not destroy the stream')
})

test('pipeUdpFramedServer - cleanup only runs once no matter how many teardown events fire', async function (t) {
  const stream = fakeSocket()
  const { logger, calls } = createLogger()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1', logger })

  stream.emit('close')
  stream.emit('close')
  stream.emit('end')
  await tick()

  t.is(calls.info.filter((args) => /Destroying pipe/.test(args[0])).length, 1)
})

test('pipeUdpFramedServer - works with the default no-op logger', async function (t) {
  const stream = fakeSocket()
  pipeUdpFramedServer(stream, null, { port: 1, host: '127.0.0.1' })

  const finished = new Promise((resolve) => stream.once('finish', resolve))
  stream.emit('end')
  await finished

  t.absent(stream.destroyed)
})
