const RAM = require('random-access-memory')
const Hypercore = require('hypercore')
const Autochannel = require('./autochannel')

// range()
main().then(() => console.log('finished')).catch(console.log)

async function main () {
  const al = new Hypercore(() => new RAM(), { valueEncoding: 'json' })
  const bl = new Hypercore(() => new RAM(), { valueEncoding: 'json' })

  await al.ready()
  await bl.ready()

  const ar = new Hypercore(() => new RAM(), bl.key, { valueEncoding: 'json' })
  const br = new Hypercore(() => new RAM(), al.key, { valueEncoding: 'json' })

  await ar.ready()
  await br.ready()

  const a = new Autochannel(al, ar)
  const b = new Autochannel(bl, br)

  replicate(al, br)
  replicate(ar, bl)

  let ai = 0
  let bi = 0

  await a.append(ai++)
  await a.append(ai++)
  await a.append(ai++)
  await a.append(ai++)

  await new Promise(r => setTimeout(r, 1000))

  await b.append(bi++)
  await b.append(bi++)
  await b.append(bi++)

  await b.append(bi++)
  await b.append(bi++)
  await b.append(bi++)

  // await new Promise(r => setTimeout(r, 1000))

  await a.append(ai++)
  await a.append(ai++)
  await a.append(ai++)
  await a.append(ai++)

  await new Promise(r => setTimeout(r, 3000))

  console.log(await a.next())
  console.log(a.remote.length)
}

async function replicate (a, b) {
  const as = a.replicate(true)
  const bs = b.replicate(false)

  as.pipe(bs).pipe(as)
}
