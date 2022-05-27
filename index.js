const { Readable } = require('streamx')
const Corestore = require('corestore')
const debounceify = require('debounceify')
const Accumulator = require('accumulator-hash')
const c = require('compact-encoding')
const { compile } = require('compact-encoding-struct')

const clockEncoding = compile({
  key: c.fixed32,
  local: c.uint,
  remote: c.uint
})

const valueEncoding = compile({
  type: c.string,
  clock: clockEncoding,
  data: c.buffer,
  eventId: c.fixed32
})

module.exports = class OpLog extends Readable {
  constructor (id, opts) {
    super()

    this.id = id

    this.store = opts.store
      ? opts.store.namespace('oplog' + this.id)
      : new Corestore(opts.storage)

    this.local = this.store.get({ name: 'local', valueEncoding })
    this.remote = null

    this.remoteIndex = 0

    this._opened = false
    this.updating = false
    this.applying = null

    this.dataEncoding = opts.encoding || null

    this.log = this.store.get({ name: 'log' })

    this._queue = []
    this._append = debounceify(this._appendBatch.bind(this))

    this.buffer = []

    this._acc = new Accumulator()
  }

  async loadRemote (remoteFeedKey) {
    await this.local.ready()
    await this.log.ready()

    this.remote = this.store.get({ key: remoteFeedKey, valueEncoding })
    await this.remote.ready()

    this.emit('live')
    this._opened = true

    this._listen()
  }

  decode (val) {
    if (this.dataEncoding) {
      val.data = c.decode(this.dataEncoding, val.data)
    } else {
      val.data = decode(val.data)
    }

    return val
  }

  // Get a deterministically ordered list of entries occuring between
  // two given clocks. The writer/reader is needed in order to know
  // who appended to the oplog last according to the `end` clock
  async * getState (start, end) {
    if (start === null && end === null) return

    // deterministically choose key if end is not provided
    const writersKey = end
      ? end.key
      : Buffer.compare(this.remote.key, this.local.key) > 0
        ? this.local.key
        : this.remote.key

    const seq = order(start, writersKey)
    const stop = end
      ? order(end, writersKey)
      : current.apply(this, [writersKey])

    let [w, r] = [this.local, this.remote]
    if (Buffer.compare(writersKey, this.local.key)) [w, r] = [r, w]

    const wopts = { start: seq.writer, end: stop.writer }
    const ropts = { start: seq.reader, end: stop.reader }

    const writer = w.createReadStream(wopts)[Symbol.asyncIterator]()
    const reader = r.createReadStream(ropts)[Symbol.asyncIterator]()

    let left = await writer.next()
    let right = await reader.next()

    while (!left.done || !right.done) {
      const l = left.done ? null : left.value
      const r = right.done ? null : right.value

      const next = left.done ? -1 : right.done ? 1 : compare(l.clock, r.clock)

      if (next >= 0) {
        yield this.decode(l)
        left = await writer.next()
      } else if (next < 0) {
        yield this.decode(r)
        right = await reader.next()
      }
    }

    // order the clock from the writer's perspective
    function order (clock, key, stop = false) {
      if (!clock) return { writer: 0, reader: 0 }

      const theirClock = Buffer.compare(clock.key, key)
      const reader = theirClock ? clock.local : clock.remote
      const writer = theirClock ? clock.remote : clock.local

      return { writer, reader }
    }

    function compare (a, b) {
      // if the reader has seen later of writers
      if (a.local < b.remote) return 1
      // if the writer has seen later of readers
      if (a.remote > b.local) return -1
      return 0
    }

    function current (key) {
      if (Buffer.compare(key, this.local.key)) {
        return {
          writer: this.remote.length,
          reader: this.local.length
        }
      }
      return {
        writer: this.local.length,
        reader: this.remote.length
      }
    }
  }

  hash (data) {
    return this._acc.hash(data)
  }

  // return -1 if the a is fully behind b
  // return 0 if the clocks reference the same point
  // return 1 if a references any updates that b doesn't
  compare (a, b) {
    if (!a) return -1
    if (!b) return 1

    const same = a.key.equals(b.key)

    const bLocal = b[same ? 'local' : 'remote']
    const bRemote = b[same ? 'remote' : 'local']

    if (a.local > bLocal) return 1
    if (a.remote > bRemote) return 1
    if (a.local < bLocal) return -1
    if (a.remote < bRemote) return -1
    return 0
  }

  // returns the union of two clocks:
  // union([1, 3], [2, 2]) -> [2, 3]
  union (a, b) {
    if (a == null) return b
    if (b == null) return a

    const same = !Buffer.compare(a.key, b.key)

    return {
      key: a.key,
      local: Math.max(a.local, same ? b.local : b.remote),
      remote: Math.max(a.remote, same ? b.remote : b.local)
    }
  }

  isLocal (clock) {
    return Buffer.compare(clock.key, this.local.key) === 0
  }

  _listen (signal) {
    this.remote.on('append', getRemote.bind(this))

    async function getRemote () {
      while (this.remoteIndex < this.remote.length) {
        const block = await this.remote.get(this.remoteIndex++)
        this.emit('data', this.decode(block))
      }
    }
  }

  async get (clock) {
    if (!this._opened) return // for testing

    const remote = Buffer.compare(clock.key, this.local.key)
    const feed = remote ? this.remote : this.local

    return feed.get(clock.local)
  }

  async _enqueue (data) {
    return new Promise((resolve, reject) => {
      this._queue.push({ data, promise: { resolve, reject } })
      this._append()
    })
  }

  async append (type, value) {
    if (!this._opened) return // for testing

    const data = this.dataEncoding
      ? c.encode(this.dataEncoding, value)
      : Buffer.from(JSON.stringify(value))

    const entry = {
      type,
      data,
      clock: this.clock()
    }

    entry.eventId = this._getEventId(entry)
    await this._enqueue(entry)

    return entry.eventId // should we return seq as well?
  }

  _getEventId (details) {
    const enc = compile({
      type: c.string,
      data: c.buffer,
      clock: clockEncoding
    })

    const digest = c.encode(enc, details)
    return this.hash(digest)
  }

  _appendBatch () {
    const batch = this._queue
    this._queue = []

    let seq = this.local.length

    return this.local.append(batch.map(e => e.data)).then(() => {
      for (const { promise } of batch) {
        promise.resolve(seq++)
      }
    })
  }

  clock () {
    return {
      key: this.local.key,
      remote: this.remote ? this.remote.length : 0,
      local: this.local.length
    }
  }
}

// yolo json encoding
function decode (o) {
  if (o instanceof Uint8Array) return decode(JSON.parse(o))
  if (o == null) return null
  if (Array.isArray(o)) return o.map(decode)
  if (o && o.type === 'Buffer') return Buffer.from(o.data)

  const ret = {}
  for (const [k, v] of Object.entries(o)) {
    if (typeof v === 'object') {
      if (v == null) continue
      ret[k] = decode(v)
    } else ret[k] = v
  }

  return ret
}
