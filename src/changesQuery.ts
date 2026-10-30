/*!
 * Copyright (c) 2026 Interop Alliance. All rights reserved.
 */
/**
 * The pull side of the PWS replication driver: the `changes`-feed request and
 * response mapping, and the RxDB pull handler built from it. Maps each wire
 * document (`{ id, _deleted, updatedAt, updatedAtCounter, originId, meta?,
 * data?, custom?, epoch?, createdBy?, writerId?, etag?, metaEtag? }`) into an
 * RxDB `WithDeleted<SyncedDoc>`, and applies the empty-page `checkpoint: null`
 * rule. The feed's `writerId` label is carried into the row like `createdBy`,
 * on tombstones too.
 */
import { isSyncCheckpointError } from '@interop/pws-client/sync'
import type {
  ReplicationCheckpoint,
  SyncCheckpoint,
  SyncedDoc,
  WasSyncPort,
  WireDoc,
  WithDeleted
} from './types.js'
import { copyOptionalBodyFields } from './types.js'
import { log } from './log.js'

/**
 * Maps one `changes`-feed wire document into a replica document. The envelope
 * fields map straight across; the content body stays under `data` (omitted for
 * tombstones, which carry no `data`) and the metadata body under `custom`. The
 * content write stamp (`updatedAt`, `updatedAtCounter`, `originId`) and the
 * nested `meta` stamp are carried verbatim; `meta` / `custom` are present only
 * once metadata has been written for the resource, and are simply absent
 * otherwise. The server-managed `createdBy` creator DID is carried across on
 * live documents and tombstones alike (it rides the feed on a delete too). The
 * `writerId` label the latest revision was declared under is carried the same
 * way, a tombstone's being the deleting request's label, and so is the opaque
 * `epoch` key-epoch id; each is simply absent when the server holds none. The
 * opaque `etag` / `metaEtag` validators are carried across the same way, so a
 * later push can echo one back verbatim as `ifMatch` without a separate
 * re-read. `_deleted` becomes RxDB's native deleted flag.
 *
 * @param doc {WireDoc}
 * @returns {WithDeleted<SyncedDoc>}
 */
export function wireDocToRxDoc(doc: WireDoc): WithDeleted<SyncedDoc> {
  const rxDoc: WithDeleted<SyncedDoc> = {
    id: doc.id,
    updatedAt: doc.updatedAt,
    _deleted: doc._deleted
  }
  copyOptionalBodyFields({ source: doc, target: rxDoc })
  return rxDoc
}

/**
 * Builds the RxDB pull handler that fetches one `changes` page per call and
 * resumes from the previous checkpoint. RxDB passes the last stored
 * {@link ReplicationCheckpoint} (`undefined` on the first pull) and the batch
 * size; the handler unwraps the opaque string for the port, which omits an
 * absent checkpoint from the request, and wraps the page's checkpoint for
 * RxDB on the way back.
 *
 * The empty-page rule: when the server returns `checkpoint: null` (no change),
 * keep the checkpoint RxDB gave us rather than persisting `null`, so the next
 * pull resumes from the same position instead of restarting the feed. The rule
 * is keyed on the RESPONSE checkpoint being nullish rather than on the page
 * being empty, so a page carrying documents with no checkpoint keeps resuming
 * too.
 *
 * The checkpoint is opaque, passed back verbatim. A server refuses a
 * checkpoint it did not issue (a replica created against another server), and
 * the port raises that as its refused-checkpoint signal (matched by
 * `isSyncCheckpointError`). The handler then pulls again from the beginning, which is safe because the apply path is keyed by
 * resource id. RxDB persists a checkpoint only with a non-empty page, so when
 * the restarted feed is empty the refused checkpoint stays stored and RxDB
 * offers it again on the next poll; the handler remembers the refusal and
 * skips straight to the restart, so the 400 round trip is paid once per
 * replication rather than once per poll. The refusal is forgotten once a
 * non-empty page hands RxDB a checkpoint to persist, since RxDB then stops
 * offering the refused one. A checkpoint is a deterministic encoding of a feed
 * position, so a Collection deleted and re-created at the same URL can later
 * issue the byte-identical string legitimately; remembered for the handler's
 * lifetime, that string would restart the feed on every poll it was offered.
 *
 * The restart clears nothing. A row that existed only before a Collection was
 * deleted and re-created stays in the replica. So does a row the new server
 * never held. Such a row stays until a consumer forgets the replica
 * (ARCHITECTURE.md documents the residual).
 *
 * @param port {WasSyncPort}
 * @returns {(lastCheckpoint: ReplicationCheckpoint | undefined, batchSize: number) =>
 *   Promise<{ documents: WithDeleted<SyncedDoc>[], checkpoint: ReplicationCheckpoint | undefined }>}
 */
export function createPullHandler(port: WasSyncPort) {
  let refused: SyncCheckpoint | undefined
  return async function pull(
    lastCheckpoint: ReplicationCheckpoint | undefined,
    batchSize: number
  ): Promise<{
    documents: WithDeleted<SyncedDoc>[]
    checkpoint: ReplicationCheckpoint | undefined
  }> {
    // Omit `checkpoint` entirely when there is none to resume from (the port
    // sends no checkpoint field, not `null`).
    const fetchPage = (checkpoint?: SyncCheckpoint) =>
      port.query({
        ...(checkpoint !== undefined && { checkpoint }),
        limit: batchSize
      })
    let resumeFrom = lastCheckpoint?.checkpoint
    if (resumeFrom === refused) {
      resumeFrom = undefined
    }
    let response
    try {
      response = await fetchPage(resumeFrom)
    } catch (err) {
      if (resumeFrom === undefined || !isSyncCheckpointError(err)) {
        throw err
      }
      // The server did not issue this checkpoint: restart the feed, and do
      // not hand the refused checkpoint back should the restarted page be
      // empty.
      log.warn(
        'The server refused the stored checkpoint; restarting the feed from the beginning',
        { err }
      )
      refused = resumeFrom
      resumeFrom = undefined
      response = await fetchPage()
    }
    // Empty page (`checkpoint: null`) means "no change": keep the prior
    // checkpoint so the feed does not restart from the beginning.
    const next = response.checkpoint ?? resumeFrom
    // A non-empty page's checkpoint is what RxDB persists and offers next, so
    // the refused one is no longer in play (the header's refusal note).
    if (response.checkpoint !== null && response.checkpoint !== undefined) {
      refused = undefined
    }
    return {
      documents: response.documents.map(wireDocToRxDoc),
      checkpoint: next === undefined ? undefined : { checkpoint: next }
    }
  }
}
