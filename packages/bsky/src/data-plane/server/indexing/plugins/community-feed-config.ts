import { Cid } from '@atproto/lex'
import { AtUri } from '@atproto/syntax'
import { community } from '../../../../lexicons/index.js'
import { BackgroundQueue } from '../../background.js'
import { DatabaseSchema } from '../../db/database-schema.js'
import { Database } from '../../db/index.js'
import { RecordProcessor } from '../processor.js'

const insertFn = async (
  _db: DatabaseSchema,
  _uri: AtUri,
  _cid: Cid,
  _obj: community.blacksky.feed.config.Main,
  _timestamp: string,
): Promise<true> => true

const findDuplicate = async (): Promise<AtUri | null> => null

const notifsForInsert = () => []

const deleteFn = async (
  _db: DatabaseSchema,
  _uri: AtUri,
): Promise<true> => true

const notifsForDelete = () => ({ notifs: [], toDelete: [] })

export type PluginType = ReturnType<typeof makePlugin>
export const makePlugin = (
  db: Database,
  background: BackgroundQueue<Database>,
) => {
  return new RecordProcessor(db, background, {
    schema: community.blacksky.feed.config.main,
    insertFn,
    findDuplicate,
    deleteFn,
    notifsForInsert,
    notifsForDelete,
  })
}

export default makePlugin
