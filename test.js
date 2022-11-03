const ram = require('random-access-memory')
const Autochannel = require('./autochannel')

// range()
main().then(() => console.log('finished')).catch(console.log)

async function main () {
  const a = new Autochannel(null, { storage: () => new ram() })
  const b = new Autochannel(null, { storage: () => new ram() })

  await a.local.ready()
  await b.local.ready()

  await Promise.all([
    a.loadRemote(b.local.key),
    b.loadRemote(a.local.key)
  ])

  replicate(a, b)

  startA()
  startB()
  read()

  let ai = 0
  let bi = 0

  await a.append('data', ai++)
  await a.append('data', ai++)
  await a.append('data', ai++)
  await a.append('data', ai++)

  await new Promise(r => setTimeout(r, 1000))

  await b.append('data', bi++)
  await b.append('data', bi++)
  await b.append('data', bi++)

  await new Promise(r => setTimeout(r, 1000))
  await b.flush()
  await new Promise(r => process.nextTick(r))
  await a.flush()

  for await (const c of a.commit()) console.log('c', c)

  await a.append('data', ai++)
  await a.append('data', ai++)
  await a.append('data', ai++)
  await a.append('data', ai++)

  await new Promise(r => setTimeout(r, 1000))

  await b.append('data', bi++)
  await b.append('data', bi++)
  await b.append('data', bi++)
  
  b.accept(1)
  b.accept(2)
  a.accept(2)

  await b.flush()
  await a.flush()

  for await (const c of a.commit()) console.log('c', c)

  async function read () {
    for await (const b of a.read(undefined, { live: true })) {
      console.log(`--- ${b.remote ? 'remote' : 'local'} ${b.seq}`)
    }
  }
  async function startA () {
    for await (const r of b.requests({ live: true })) {
      if (r.block.data % 2) b.accept(r.seq)
    }
  }

  async function startB () {
    for await (const r of a.requests({ live: true })) {
      if (r.block.data % 2 === 0) a.accept(r.seq)
    }
  }
}

function newEntry (n) {
  return {
    type: 'data',
    value: n
  }
}

async function replicate (a, b) {
  const as = a.store.replicate(true)
  const bs = b.store.replicate(false)

  as.pipe(bs).pipe(as)
}

function range () {
  const a = [1,2,3,6,7,9,11,14,15]
  console.log(getRanges(a))
}

function getRanges (seqs) {
  const arr = [...seqs].sort((a, b) => a - b)

  const ranges = []

  let a = arr[0]
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