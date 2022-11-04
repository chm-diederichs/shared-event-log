module.exports = class Autochannel {
  constructor (local, remote, opts = {}) {
    this.local = local
    this.remote = remote

    this.initiator = Buffer.compare(this.local.key, this.remote.key) < 0

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

  async ready () {
    await this.local.ready()
    await this.remote.ready()
  }

  async append (data) {
    const entry = {
      type: 'data',
      data,
      remoteLength: this.clock()
    }

    return this.local.append(entry)
  }

  async next (prev = this.prev) {
    const local = getSeqIterator(this.local, prev.local)
    const remote = getSeqIterator(this.remote, prev.remote)

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
      heads.sort((a, b) => a.length - b.length)
      for (const head of heads) {
        while (head.length) batch.push(head.shift())
      }
    }

    this.prev = {
      local: this.local.length,
      remote: this.remote.lengthss
    }

    return batch
  }

  async commit (sig) {
    const entry = {
      type: 'commit',
      sig,
      remoteLength: this.clock()
    }

    return this.local.append(entry)
  }

  clock () {
    return this.remote ? this.remote.length : 0
  }
}

function getSeqIterator (feed, start, opts) {
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
