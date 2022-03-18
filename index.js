const { Readable } = require('streamx')
const Corestore = require('corestore')
const debounceify = require('debounceify')

module.exports = class OpLog extends Readable {
  constructor (id, opts) {
    super()

    this.id = id

    this.store = opts.store
      ? opts.store.namespace('oplog' + this.id)
      : new Corestore(opts.storage)

    this.local = this.store.get({ name: 'local' })
    this.remote = null

    this.remoteIndex = 0

    this._opened = false
    this.updating = false
    this.applying = null

    this.log = this.store.get({ name: 'log' })

    this._queue = []
    this._append = debounceify(this._appendBatch.bind(this))

    this.buffer = []
  }

  async loadRemote (remoteFeedKey) {
    await this.local.ready()
    await this.log.ready()

    this.remote = this.store.get(remoteFeedKey)
    await this.remote.ready()

    this.emit('open')
    this._opened = true

    this._listen()
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

    let [writer, reader] = [this.local, this.remote]
    if (Buffer.compare(writersKey, this.local.key)) {
      [writer, reader] = [reader, writer]
    }

    const seq = order(start, writersKey)
    const stop = end
      ? order(end, writersKey)
      : current.apply(this, [writersKey])

    let left = null // from writer's feed
    let right = null // from reader's feed

    while (true) {
      if (!left && seq.writer < stop.writer) {
        left = JSON.parse(await writer.get(seq.writer++))
      }

      if (!right && seq.reader < stop.reader) {
        right = JSON.parse(await reader.get(seq.reader++))
      }

      if (left === null & right === null) break

      const next = left ? (right ? compare(left.clock, right.clock) : 1) : -1

      if (next >= 0) {
        yield decode(left)
        left = null
      } else if (next <= 0) {
        yield decode(right)
        right = null
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

  compare (a, b) {
    if (!a) return 1
    if (!b) return -1

    const same = Buffer.compare(a.key, b.key) === 0

    const bLocal = b[same ? 'local' : 'remote']
    const bRemote = b[same ? 'remote' : 'local']

    if (a.local < bLocal) return 1
    if (a.local > bLocal) return -1
    if (a.remote < bRemote) return 1
    if (a.remote > bRemote) return -1
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
        this.emit('data', decode(block))
      }
    }
  }

  async get (clock) {
    if (!this._opened) return // for testing

    const remote = Buffer.compare(clock.key, this.local.key)
    const feed = remote ? this.remote : this.local

    return feed.get(clock.local)
  }

  async append (type, data) {
    if (!this._opened) return // for testing

    const entry = {
      type,
      data,
      clock: this.clock()
    }

    this._queue.push(JSON.stringify(entry))
    return this._append()
  }

  _appendBatch () {
    const batch = this._queue
    this._queue = []
    return this.local.append(batch)
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

  const ret = {}
  for (const [k, v] of Object.entries(o)) {
    if (typeof v === 'object') {
      if (v == null) continue
      if (v && v.type === 'Buffer') {
        ret[k] = Buffer.from(v.data)
      } else ret[k] = decode(v)
    } else ret[k] = v
  }
  return ret
}
