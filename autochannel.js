module.exports = class Autochannel {
  constructor (local, remote, opts = {}) {
    this.local = local
    this.remote = remote

    this.isInitiator = Buffer.compare(local.key, remote.key) < 0

    this.initiator = this.isInitiator ? local : remote
    this.responder = this.isInitiator ? remote : local

    this.prev = {
      initiator: 0,
      responder: 0
    }
  }

  async ready () {
    await this.local.ready()
    await this.remote.ready()
  }

  async append (op, commitment = this.isInitiator) {
    const remoteLength = this.remote.length

    const entry = {
      op,
      commitment,
      remoteLength
    }

    return this.local.append(entry)
  }

  async * accepted () {
    const resp = getForwardIterator(this.responder)
    const init = this.initiator.createReadStream({ live: true })

    let r = null
    let batch = []

    let lseq = 0
    for await (const l of init) {
      if ((r?.seq || 0) >= l.remoteLength - 1) {
        this.prev.initiator++
        yield l
        continue
      }

      // keep looping until we reach head
      while (true) {
        r = await resp.next()
        batch.push(r.value)

        if (r.value.commitment) {
          while (batch.length) {
            this.prev.responder++
            yield batch.shift()
          }
        }

        // latest is elsewhere
        if (r.seq === l.remoteLength - 1) break
        if (r.seq === this.responder.length - 1) break
      }

      while (batch.length) {
        this.prev.responder++
        yield batch.shift()
      }

      this.prev.initiator++
      yield l
    }
  }

  async next (prev = this.prev) {
    const init = getReverseIterator(this.initiator, prev.initiator)
    const resp = getReverseIterator(this.responder, prev.responder)

    const batch = []
    const heads = [[], []]

    let l = await init.next()
    let r = await resp.next()

    while (!l.done || !r.done) {
      const lstop = (r.value?.remoteLength || 0) - 1
      const rstop = (l.value?.remoteLength || 0) - 1

      const [left, right] = heads

      // keep going back until we get to last seen length
      while (!l.done && l.seq > lstop) {
        left.push({
          value: l.value,
          seq: l.seq,
          remote: this.isInitiator
        })
        l = await init.next()
      }

      // keep going back until we get to last seen length
      while (!r.done && r.seq > rstop) {
        right.push({
          value: r.value,
          seq: r.seq,
          remote: !this.isInitiator
        })
        r = await resp.next()
      }

      // initiator first
      if (!this.isInitiator) heads.reverse()
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
      if (seq <= start || seq === 0) return { value: null, done: true }
      const value = await feed.get(--seq)
      return {
        value,
        seq,
        done: false
      }
    }
  }
}
