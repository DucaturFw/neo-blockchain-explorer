console.log("starting grabber")

let NEO_NODE = process.env.NEO_EXPLORER_NEO_NODE || ""
let DB_NAME = process.env.NEO_EXPLORER_DB_NAME || ""
let TABLE_BLOCKS = process.env.NEO_EXPLORER_TABLE_BLOCKS || "neo_blocks"
let TABLE_TXS = process.env.NEO_EXPLORER_TABLE_TXS || "neo_txs"
let RETHINK_URI = process.env.NEO_EXPLORER_RETHINK || ""

console.assert(NEO_NODE, "please provide $NEO_EXPLORER_NEO_NODE!")
console.assert(DB_NAME, "please provide $NEO_EXPLORER_DB_NAME!")
console.assert(TABLE_BLOCKS, "please provide $NEO_EXPLORER_TABLE_BLOCKS!")
console.assert(TABLE_TXS, "please provide $NEO_EXPLORER_TABLE_TXS!")

console.log(`neo node: ${NEO_NODE}\ndb name: ${DB_NAME}\nblocks table: ${TABLE_BLOCKS}\ntxs table: ${TABLE_TXS}\nrethink: ${RETHINK_URI}`)

import axios from "axios"
import r from "rethinkdb"

let api = <T>(method: string, params: any = []) => axios
	.post(NEO_NODE, { jsonrpc: "2.0", id: 1, method, params })
	.then(res => res.data)
	.then(data => { if (data.error) throw data.error; else return data.result as T })

const delay = (time: number) => new Promise(resolve => setTimeout(resolve, time))

;(async function()
{
	let conn = await r.connect(RETHINK_URI)
	
	// init db
	let dbs = await r.dbList().run(conn)
	if (dbs.indexOf(DB_NAME) == -1)
		await r.dbCreate(DB_NAME).run(conn)

	let db = r.db(DB_NAME)
	let init = async () =>
	{
		let tables = await db.tableList().run(conn)
		if (tables.indexOf(TABLE_BLOCKS) == -1)
			await db.tableCreate(TABLE_BLOCKS, { primaryKey: "hash" }).run(conn)
		if ((await db.table(TABLE_BLOCKS).indexList().run(conn)).indexOf('index') == -1)
		{
			await db.table(TABLE_BLOCKS).indexCreate('index').run(conn)
			await db.table(TABLE_BLOCKS).indexWait('index').run(conn)
		}
		if (tables.indexOf(TABLE_TXS) == -1)
			await db.tableCreate(TABLE_TXS, { primaryKey: "txid" }).run(conn)
	}
	await init()
	
	let getBlocks = async () =>
	{
		let blockHeight = await api<number>("getblockcount")
		console.log(`block height: ${blockHeight}`)
		let lastBlock = await r.branch(
			db.table(TABLE_BLOCKS).isEmpty(),
				-1,
				db.table(TABLE_BLOCKS)
					.max({ index: 'index' })('index')
					.default(-1)
		).run(conn)

		console.log(`last block: ${lastBlock}`)
		if (blockHeight <= lastBlock)
			return console.error("chain is unsynced"), delay(30000)

		const BATCH_SIZE = blockHeight - lastBlock
		
		for (let i = 0; i < BATCH_SIZE; i++)
		{
			let idx = lastBlock + 1 + i
			if (idx == blockHeight)
				return delay(3000)

			const block = await api("getblock", [idx, 1])
			// console.log(block)
			await db.table(TABLE_BLOCKS).insert(block).run(conn)
		}
	}
	let blocks = await db.table(TABLE_BLOCKS)
		.changes({ includeInitial: true })
		.map(x => x('new_val'))
		.concatMap(x => <any>x('tx'))
		.filter(a => db.table(TABLE_TXS).getAll(a('txid')).isEmpty())
		.run(conn) as r.CursorResult<{ txid: string }>

	blocks.each((err, a) => (console.log(a.txid), db.table(TABLE_TXS).insert(a).run(conn)))

	while(true)
	{
		await getBlocks()
	}
})()