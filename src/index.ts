console.log("starting grabber")

let NEO_NODE = "http://18.222.114.103:30333"
let TABLE_BLOCKS = "neo_blocks"
let TABLE_TXS = "neo_txs"
let DB_NAME = "test"

import axios from "axios"
import r from "rethinkdb"

let api = <T>(method: string, params: any = []) => axios
	.post(NEO_NODE, { jsonrpc: "2.0", id: 1, method, params })
	.then(res => res.data)
	.then(data => { if (data.error) throw data.error; else return data.result as T })

const delay = (time: number) => new Promise(resolve => setTimeout(resolve, time))

;(async function()
{
	let conn = await r.connect(NEO_NODE.replace(/^http\:\/\//, '').replace(/\:\d+$/, ''))
	let db = r.db(DB_NAME)
	let init = async () =>
	{
		let tables = await db.tableList().run(conn)
		if (tables.indexOf(TABLE_BLOCKS) == -1)
			await db.tableCreate(TABLE_BLOCKS, { primaryKey: "hash" }).run(conn)
		if (tables.indexOf(TABLE_TXS) == -1)
			await db.tableCreate(TABLE_TXS, { primaryKey: "txid" }).run(conn)
	}
	await init()
	
	let getBlocks = async () =>
	{
		let blockHeight = await api<number>("getblockcount")
		console.log(`block height: ${blockHeight}`)
		let lastBlock = await db.table(TABLE_BLOCKS).max('index')('index').default(-1).run(conn)

		console.log(`last block: ${lastBlock}`)
		console.assert(blockHeight >= lastBlock, "chain is fucking unsynced")
		if (blockHeight == lastBlock)
			return delay(30000)
		
		const block = await api("getblock", [lastBlock + 1, 1])
		// console.log(block)
		await db.table(TABLE_BLOCKS).insert(block).run(conn)
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