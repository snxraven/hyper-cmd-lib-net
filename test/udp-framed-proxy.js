const test = require('brittle')
const { createUdpFramedProxy } = require('../lib/udp-framed.js')
const { echoStream, createLogger, tick, bindUdpClient } = require('./helpers.js')

test('createUdpFramedProxy - round-trips a datagram through the tunnel factory', async function (t) {
  const proxy = createUdpFramedProxy(
    () => echoStream(),
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  const reply = new Promise((resolve) => client.once('message', (msg) => resolve(msg.toString())))
  const payload = Buffer.from('hello-udp')
  client.send(payload, 0, payload.length, addr.port, '127.0.0.1')

  t.is(await reply, 'hello-udp')
})

test('createUdpFramedProxy - reuses the same tunnel for repeated datagrams from one client', async function (t) {
  let calls = 0
  const proxy = createUdpFramedProxy(
    () => {
      calls++
      return echoStream()
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  const send = (msg) =>
    new Promise((resolve) => {
      client.once('message', (m) => resolve(m.toString()))
      client.send(Buffer.from(msg), 0, msg.length, addr.port, '127.0.0.1')
    })

  t.is(await send('first'), 'first')
  t.is(await send('second'), 'second')
  t.is(calls, 1)
  t.is(proxy.clients.size, 1)
})

test('createUdpFramedProxy - gives distinct clients independent tunnels and replies', async function (t) {
  let calls = 0
  const proxy = createUdpFramedProxy(
    () => {
      calls++
      return echoStream()
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const c1 = await bindUdpClient()
  const c2 = await bindUdpClient()
  t.teardown(() => {
    c1.close()
    c2.close()
  })

  const r1 = new Promise((resolve) => c1.once('message', (m) => resolve(m.toString())))
  const r2 = new Promise((resolve) => c2.once('message', (m) => resolve(m.toString())))
  c1.send(Buffer.from('from-c1'), 0, 7, addr.port, '127.0.0.1')
  c2.send(Buffer.from('from-c2'), 0, 7, addr.port, '127.0.0.1')

  t.is(await r1, 'from-c1')
  t.is(await r2, 'from-c2')
  t.is(calls, 2)
})

test('createUdpFramedProxy - oversized reply datagram destroys the tunnel and evicts the client', async function (t) {
  let calls = 0
  const { logger, calls: logs } = createLogger()
  const proxy = createUdpFramedProxy(
    () => {
      calls++
      return echoStream()
    },
    { port: 0, host: '127.0.0.1', maxFrameSize: 5, logger },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  const oversized = Buffer.from('0123456789') // 10 bytes > maxFrameSize(5)
  client.send(oversized, 0, oversized.length, addr.port, '127.0.0.1')
  await tick(30)

  t.is(proxy.clients.size, 0, 'client entry removed after oversized datagram')
  t.ok(logs.error.some((args) => /Datagram too large/.test(args[0])))

  const small = Buffer.from('small')
  client.send(small, 0, small.length, addr.port, '127.0.0.1')
  await tick(30)

  t.is(calls, 2, 'a fresh tunnel is created for the same client after eviction')
})

test('createUdpFramedProxy - removes the client entry when its tunnel closes', async function (t) {
  let tunnel
  const proxy = createUdpFramedProxy(
    () => {
      tunnel = echoStream()
      return tunnel
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  const payload = Buffer.from('hi')
  client.send(payload, 0, payload.length, addr.port, '127.0.0.1')
  await tick(30)
  t.is(proxy.clients.size, 1)

  tunnel.emit('close')
  await tick()
  t.is(proxy.clients.size, 0)
})

test('createUdpFramedProxy - does not turn reliable stream bytes into datagrams', async function (t) {
  let tunnel
  const proxy = createUdpFramedProxy(
    () => {
      tunnel = echoStream()
      return tunnel
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  const received = []
  client.on('message', (msg) => received.push(msg.toString()))

  const payload = Buffer.from('only-this')
  client.send(payload, 0, payload.length, addr.port, '127.0.0.1')
  await tick(30)

  tunnel.push(Buffer.from('should-not-leak'))
  await tick(30)

  t.alike(received, ['only-this'])
})

test('createUdpFramedProxy - holds datagrams until the tunnel connects', async function (t) {
  let tunnel
  const proxy = createUdpFramedProxy(
    () => {
      tunnel = echoStream()
      tunnel.connected = false
      const send = tunnel.trySend
      tunnel.sentEarly = 0
      tunnel.trySend = (buf) => {
        if (tunnel.connected !== true) tunnel.sentEarly++
        send(buf)
      }
      return tunnel
    },
    { port: 0, host: '127.0.0.1' },
    () => {}
  )
  t.teardown(() => proxy.proxySocket.close())
  await tick()

  const addr = proxy.proxySocket.address()
  const client = await bindUdpClient()
  t.teardown(() => client.close())

  let early = false
  const reply = new Promise((resolve) => {
    client.once('message', (msg) => {
      if (!tunnel.connected) early = true
      resolve(msg.toString())
    })
  })
  const payload = Buffer.from('held')
  client.send(payload, 0, payload.length, addr.port, '127.0.0.1')
  await tick(30)

  t.is(tunnel.sentEarly, 0)
  t.absent(early)

  tunnel.connected = true
  tunnel.emit('connect')

  t.is(await reply, 'held')
  t.absent(early)
})
