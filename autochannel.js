const { Readable } = require('streamx')
const sodium = require('sodium-universal')
const Corestore = require('corestore')
const debounceify = require('debounceify')
const Accumulator = require('accumulator-hash')

module.exports = class Autochannel {
  constructor (id, opts = {}) {
    if (!id) {
      const seed = Buffer.alloc(32)
      sodium.randombytes_buf(seed)
      id = seed.toString('hex')
    }

    this.store = opts.store
      ? opts.store.namespace('oplog' + id)
      : new Corestore(opts.storage)

    this.local = this.store.get({ name: 'local', valueEncoding: 'json' })
    this.remote = null

    this.initiator = false

    this.remoteIndex = opts.remoteIndex || 0

    this.updating = false
    this.applying = null

    this.remoteAccept = 0
    this.localAccept = 0

    this.onAccept = opts.onAccept
    this.onCommit = opts.onCommit

    this.prev = { local: 0, remote: 0 }
    this._pendingAccepts = new Set()
  }

  async loadRemote (remoteFeedKey) {
    await this.local.ready()

    this.remote = this.store.get(remoteFeedKey)
    await this.remote.ready()

    this.initiator = Buffer.compare(this.local.key, this.remote.key) < 0
  }

  async append (type, data) {
    const entry = {
      type,
      data,
      remoteLength: this.clock()
    }

    return this.local.append(entry)
  }

  async * requests (start = this.remoteIndex, opts = {}) {
    if (typeof start === 'object') {
      yield * this.requests(this.remoteIndex, start)
    }

    const str = this.remote.createReadStream({ ...opts, start })
    for await (const data of str) {
      yield {
        block: decode(data),
        seq: start++
      }
    }
  }

  async * read (prev = this.prev, opts) {
    const local = getSeqIterator(this.local, prev.local, opts)
    const remote = getSeqIterator(this.remote, prev.remote, opts)

    let l = await local.next()
    let r = await remote.next()
    r.value = decode(r.value)

    while (!l.done || !r.done) {
      // these definitely came before r
      while (!l.done && l.value.remoteLength < r.seq) {
        yield {
          value: l.value,
          seq: l.seq,
          remote: false
        }
        l = await local.next()
      }

      // these definitely came before l
      while (!r.done && r.value.remoteLength < l.seq) {
        yield {
          value: r.value,
          seq: r.seq,
          remote: true
        }
        r = await remote.next()
        r.value = decode(r.value)
      }
    }

    this.prev = {
      local: l.seq,
      remote: r.seq
    }
  }

  async * commit (prev) {
    for await (const e of this.read(prev)) {
      if (e.value.type === 'accept') yield * this.accepted(e.value.data)
    }
  }

  // takes series of indices
  async accept (seq) {
    this._pendingAccepts.add(seq)
  }

  async flush () {
    const accepted = getRanges(this._pendingAccepts)
    await this.append('accept', accepted)

    this._pendingAccepts.clear()
  }

  async * accepted (ranges, feed = this.local) {
    for (const [start, end] of ranges) {
      if (start === end) yield decode(await feed.get(start))
      else {
        for await (const v of feed.createReadStream({ start, end })) {
          yield decode(v)
        }
      }
    }
  }

  clock () {
    return this.remote ? this.remote.length : 0
  }
}

function getRanges (seqs) {
  const arr = [...seqs].sort((a, b) => a - b)

  if (!arr.length) return []

  let a = arr[0]
  if (arr.length === 1) return [[a, a]]

  const ranges = []

  let b = arr[1]
  let low = a

  for (let i = 1; i < arr.length;) {
    if ((b - a) > 1) {
      ranges.push([low, a])
      low = b
    }

    a = b
    b = arr[i++]
  }

  ranges.push([low, b])
  return ranges
}

function getSeqIterator (feed, seq, opts) {
  const str = feed.createReadStream({ ...opts, start: seq })
  const ite = str[Symbol.asyncIterator]()

  return {
    async next () {
      return {
        ...(await ite.next()),
        seq: ++seq
      }
    }
  }
}

// json reviver
function decode (o) {
  if (typeof o === 'number') return o
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
