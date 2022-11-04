const { once } = require('events')

module.exports = class Autochannel {
  constructor (local, remote, opts = {}) {
    this.local = local
    this.remote = remote

    this.isInitiator = Buffer.compare(local.key, remote.key) < 0

    this.initiator = this.isInitiator ? local : remote
    this.responder = this.isInitiator ? remote : local

    this.prev = {
      local: 0,
      remote: 0
    }
  }

  async ready () {
    await this.local.ready()
    await this.remote.ready()
  }

  async append (op, commitment = null) {
    const remoteLength = this.remote.length

    const entry = {
      op,
      commitment,
      remoteLength
    }

    if (commitment) this.prev = this.clock()

    return this.local.append(entry)
  }

  async * accepted () {
    const feeds = [this.local, this.remote]
    const initiator = feeds[this.initiator ? 0 : 1]
    const responder = feeds[this.initiator ? 1 : 0]

    const resp = getForwardIterator(this.responder)
    const init = this.initiator.createReadStream({ live: true })

    let r = null
    let batch = []

    for await (const l of init) {
      if ((r?.seq || 0) >= l.remoteLength - 1) {
        yield l
        continue
      }

      // keep looping until we reach head
      while (true) {
        r = await resp.next()
        if (r.value.commitment) {
          while (batch.length) yield batch.shift()
        } else {
          batch.push(r.value)
        }

        // latest is elsewhere
        if (r.seq === l.remoteLength - 1) break
        if (r.seq === responder.length - 1) break
      }

      while (batch.length) yield batch.shift()

      yield l
    }
  }

  async next (prev = this.prev) {
    const local = getReverseIterator(this.local, prev.local)
    const remote = getReverseIterator(this.remote, prev.remote)

    const batch = []
    const heads = [[], []]

    let l = await local.next()
    let r = await remote.next()

    while (!l.done || !r.done) {
      const lstop = (r.value?.remoteLength || 0) - 1
      const rstop = (l.value?.remoteLength || 0) - 1

      const [left, right] = heads

      // keep going back until we get to head
      while (!l.done && l.seq > lstop) {
        left.push({
          value: l.value,
          seq: l.seq,
          remote: false
        })
        l = await local.next()
      }

      // these definitely came before l
      while (!r.done && r.seq > rstop) {
        right.push({
          value: r.value,
          seq: r.seq,
          remote: true
        })
        r = await remote.next()
      }

      // shortest at top
      if (!this.initiator) heads.reverse()
      for (const head of heads) {
        while (head.length) batch.push(head.shift())
      }
    }

    return batch
  }

  clock () {
    const start = {
      local: this.local.length,
      remote: this.remote.length
    }
  }
}

function getForwardIterator (feed, start, opts) {
  let seq = feed.length
  const str = feed.createReadStream({ start, live: true })
  const ite = str[Symbol.asyncIterator]()

  return {
    async next () {
      return {
        ...(await ite.next()),
        seq: this.done ? seq : seq++,
      }
    }
  }
}

function getReverseIterator (feed, start, opts) {
  let seq = feed.length
  return {
    async next () {
      if (seq < start || seq === 0) return { value: null, done: true }
      const value = await feed.get(--seq)
      return {
        value,
        seq,
        done: false
      }
    }
  }
}
