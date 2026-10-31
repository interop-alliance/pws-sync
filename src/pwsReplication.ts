/*!
 * Copyright (c) 2026 Interop Alliance. All rights reserved.
 */
/**
 * `createPwsReplication` -- wires an RxDB collection to a remote PWS Collection
 * through the collection-agnostic pull/push handlers. This is the driver's own
 * entry point; the caller supplies an already-open RxDB collection and a
 * {@link PwsReplicationPort} (its only PWS dependency), and gets back the live
 * `RxReplicationState` to observe (`error$` / `active$`) and control
 * (`reSync()` / `cancel()`).
 *
 * No React imports -- only RxDB (the replication engine being wrapped) and the
 * injected port. This is the seam that keeps the PWS access injectable.
 */
import {
  replicateRxCollection,
  type RxReplicationState
} from 'rxdb/plugins/replication'
import type { RxCollection } from 'rxdb/plugins/core'
import { isMetaStamp, isWriteStamp } from '@interop/pws-client/sync'
import type {
  OptionalBodyFields,
  PwsReplicationPort,
  ReplicationCheckpoint,
  SyncedDoc
} from './types.js'
import { bodiesEqual } from './types.js'
import { statesEqual } from './conflictHandler.js'
import { syncedDocSchema } from './syncedDocSchema.js'
import { createPullHandler } from './changesQuery.js'
import { createPushHandler, type PushWriteAck } from './pushWrites.js'
import { log } from './log.js'

const schemaProperties: Record<string, unknown> = syncedDocSchema().properties
const metaProperties: Record<string, unknown> =
  (schemaProperties.meta as { properties?: Record<string, unknown> })
    .properties ?? {}

/**
 * Whether every string member of an acked record fits the `maxLength` its
 * schema property declares, so a patch carrying it cannot be refused by a
 * validating storage and lose the validators patched beside it. A member with
 * no bound fits anything; the first member that does not fit is logged at
 * `debug`.
 *
 * @param options {object}
 * @param options.id {string}   the row, for the log entry
 * @param options.members {object}   the acked members, by schema property name
 * @param [options.properties] {Record<string, unknown>}   the schema
 *   properties to check against; the top-level ones by default
 * @returns {boolean}
 */
function fitsSchema({
  id,
  members,
  properties = schemaProperties
}: {
  id: string
  members: object
  properties?: Record<string, unknown>
}): boolean {
  for (const [member, value] of Object.entries(members)) {
    const bound = (properties[member] as { maxLength?: unknown } | undefined)
      ?.maxLength
    if (
      typeof value === 'string' &&
      typeof bound === 'number' &&
      value.length > bound
    ) {
      log.debug('Acked member exceeds the schema bound; not patched', {
        id,
        member
      })
      return false
    }
  }
  return true
}

/**
 * Copies onto `patch` each member of `candidate` the row does not already
 * hold, so a row that is already current gets no patch and no follow-up push
 * cycle. A candidate member set to `undefined` asks for the row's member to be
 * removed, and is copied only while the row holds one.
 *
 * @param options {object}
 * @param options.patch {Record<string, unknown>}
 * @param options.current {SyncedDoc}   the row as stored
 * @param options.candidate {Partial<SyncedDoc>}   the acked members
 */
function assignDiffering({
  patch,
  current,
  candidate
}: {
  patch: Record<string, unknown>
  current: SyncedDoc
  candidate: Partial<SyncedDoc>
}): void {
  for (const [member, value] of Object.entries(candidate)) {
    if (!bodiesEqual(current[member as keyof SyncedDoc], value)) {
      patch[member] = value
    }
  }
}

/**
 * Builds the push write-back: patches an accepted write's acked state into the
 * local row so the next conditional write's `If-Match` echoes what the server
 * last reported, and so the row holds what the server assigned without waiting
 * for the feed echo. Skips rows that are gone or already current (a tombstoned
 * row is invisible to `findOne` and needs no write-back -- nothing further is
 * pushed for a deleted id).
 *
 * The validators (`etag` from the content ack, `metaEtag` from the `/meta`
 * ack) are patched whenever present. The stamp members ride beside them under
 * three conditions. A member is patched only from an ack that also carries a
 * validator: a hidden-`ETag` deployment, where the echo has to land anyway,
 * gets no write-back it did not have. Each stamp is patched as a unit: the
 * content ack supplies the top-level triple and `createdBy`, the `/meta` ack
 * supplies `meta`, and the two are not mixed, so the `/meta` ack's own copy of
 * the content stamp is ignored. And the content stamp is patched only while
 * the row still holds the state that was pushed ({@link statesEqual}), so a
 * local edit made during the push keeps its own stamp even when the edit left
 * `updatedAt` alone (the validators are patched regardless). That comparison
 * leaves the `writerId` label out: the label is not content, and an echo of
 * an earlier revision admitted during the push moves it without touching what
 * was pushed. A member longer than the schema allows is skipped at `debug`,
 * so the single write cannot throw on it.
 *
 * The `writerId` label rides beside the content validator too, though no ack
 * carries one: the server's declare-or-clear rule makes the stored label
 * known by construction. A content write declared the injected label, so the
 * row gets it; a content write without one cleared the stored label, so the
 * row's member is removed. A metadata write leaves the label alone, so the
 * `/meta` ack patches none. Everything lands in one write.
 *
 * The patch is computed twice: once against a read of the row, to skip the
 * write when nothing differs, and again inside `incrementalModify` against the
 * revision the write lands on. A local edit that lands between the read and
 * the write is seen by the second computation, so its stamp is kept rather
 * than overwritten with the superseded write's.
 *
 * A failure is logged at `warn` and swallowed: the write itself succeeded, and
 * a missed write-back only means the acked state is adopted from the change
 * feed's echo on a later pull.
 *
 * @param options {object}
 * @param options.rxCollection {RxCollection<SyncedDoc>}
 * @param [options.writerId] {string}   the label every content write and
 *   delete declares; absent when pushes declare none
 * @returns {(ack: PushWriteAck) => Promise<void>}
 */
function createAckWriteBack({
  rxCollection,
  writerId
}: {
  rxCollection: RxCollection<SyncedDoc>
  writerId?: string
}) {
  /**
   * The members of the row that differ from the acked state, by schema
   * property name, with `undefined` asking for the member's removal.
   *
   * @param options {object}
   * @param options.ack {PushWriteAck}
   * @param options.current {SyncedDoc}   the row as stored; read, never
   *   mutated
   * @returns {Record<string, unknown>}
   */
  function buildPatch({
    ack,
    current
  }: {
    ack: PushWriteAck
    current: SyncedDoc
  }): Record<string, unknown> {
    const { id, content, meta } = ack
    const patch: Record<string, unknown> = {}
    if (content?.etag !== undefined) {
      const { etag, createdBy } = content
      const candidate: Partial<SyncedDoc> = { etag }
      // The stamp is patched whole or not at all; a partial one is left out.
      // A row `findOne` returned is live, and so is one `incrementalModify`
      // hands over for a live row. The label is taken from the pushed state
      // so the comparison ignores it.
      if (
        isWriteStamp(content) &&
        statesEqual(
          { ...current, _deleted: false, writerId: ack.pushedState.writerId },
          ack.pushedState
        )
      ) {
        const { updatedAt, updatedAtCounter, originId } = content
        const stamp = { updatedAt, updatedAtCounter, originId }
        if (fitsSchema({ id, members: stamp })) {
          Object.assign(candidate, stamp)
        }
      }
      if (
        createdBy !== undefined &&
        fitsSchema({ id, members: { createdBy } })
      ) {
        candidate.createdBy = createdBy
      }
      // Declared, the label lands; undeclared, `undefined` removes the member.
      if (writerId === undefined || fitsSchema({ id, members: { writerId } })) {
        candidate.writerId = writerId
      }
      assignDiffering({ patch, current, candidate })
    }
    if (meta?.etag !== undefined) {
      const candidate: Partial<SyncedDoc> = { metaEtag: meta.etag }
      if (
        isMetaStamp(meta.meta) &&
        fitsSchema({ id, members: meta.meta, properties: metaProperties })
      ) {
        candidate.meta = meta.meta
      }
      assignDiffering({ patch, current, candidate })
    }
    return patch
  }

  return async function writeBack(ack: PushWriteAck): Promise<void> {
    try {
      const doc = await rxCollection.findOne(ack.id).exec()
      if (doc === null) {
        return
      }
      // A row that is already current gets no write and no follow-up cycle.
      const snapshot = buildPatch({ ack, current: doc.toJSON() as SyncedDoc })
      if (Object.keys(snapshot).length > 0) {
        // `incrementalPatch` cannot remove a member, so the patch is applied
        // by hand: an `undefined` value deletes the member instead. It is
        // rebuilt against `docData`, the revision this write lands on, so an
        // edit that landed since the read above keeps its own stamp.
        await doc.incrementalModify(docData => {
          const patch = buildPatch({ ack, current: docData })
          for (const [member, value] of Object.entries(patch)) {
            if (value === undefined) {
              // Only an optional member is ever asked to be removed.
              delete docData[member as keyof OptionalBodyFields]
            } else {
              Object.assign(docData, { [member]: value })
            }
          }
          return docData
        })
      }
    } catch (err) {
      // Best-effort: the server write was accepted; the feed's echo on the
      // next pull corrects the row if this local patch could not be applied.
      log.warn('Could not write the acked state back into the local row', {
        id: ack.id,
        err
      })
    }
  }
}

/**
 * Starts (or configures) replication of one RxDB collection against a remote PWS
 * Collection. Poll-based only -- no `pull.stream$` (live streaming is deferred
 * server-side); RxDB's own `retryTime` backoff and `error$` are the reachability
 * signal (the replication attempt is the probe).
 *
 * @param options {object}
 * @param options.rxCollection {RxCollection<SyncedDoc>}   the local replica
 * @param options.pwsPort {PwsReplicationPort}   injected PWS access
 * @param options.replicationIdentifier {string}   stable id (include the server
 *   URL + collection) so RxDB can resume across reloads
 * @param [options.batchSize] {number}    pull `limit` / push batch (default 100)
 * @param [options.retryTime] {number}    ms backoff between failed cycles
 * @param [options.live] {boolean}        ongoing (default true) vs one-shot
 * @param [options.autoStart] {boolean}   start immediately (default true)
 * @param [options.writerId] {string}    this replica's writer-attribution
 *   label (the PWS `writerId`), minted and kept app-side. The driver never
 *   mints, persists, or derives one. When present, every content write and
 *   delete declares it as the `Writer-Id` header; a metadata write sends none,
 *   since the label is a member of the content record alone. When absent,
 *   pushes declare no label, which clears any stored one under the server's
 *   declare-or-clear rule. A session that must not reveal a stable label to
 *   the host leaves it absent or passes a per-session one. The pull side does
 *   not read this option: the pull handler decrypts nothing (bodies are
 *   opaque), so a replica's own echo has no decrypt to skip, and RxDB writes a
 *   pulled state into the local row only where the collection's `isEqual` says
 *   it differs. The feed's label for each revision, whoever declared it, lands
 *   on the row as `SyncedDoc.writerId`; an own content write's lands from the
 *   ack write-back too ({@link createAckWriteBack}).
 * @returns {RxReplicationState<SyncedDoc, ReplicationCheckpoint>}
 */
export function createPwsReplication({
  rxCollection,
  pwsPort,
  replicationIdentifier,
  batchSize = 100,
  retryTime,
  live = true,
  autoStart = true,
  writerId
}: {
  rxCollection: RxCollection<SyncedDoc>
  pwsPort: PwsReplicationPort
  replicationIdentifier: string
  batchSize?: number
  retryTime?: number
  live?: boolean
  autoStart?: boolean
  writerId?: string
}): RxReplicationState<SyncedDoc, ReplicationCheckpoint> {
  return replicateRxCollection<SyncedDoc, ReplicationCheckpoint>({
    replicationIdentifier,
    collection: rxCollection,
    // The driver reads and writes `_deleted` everywhere (push rows, conflict
    // entries, the feed primary read), so RxDB's default `deletedField` is the
    // only one it can run under; the option is not exposed.
    live,
    autoStart,
    ...(retryTime !== undefined && { retryTime }),
    pull: {
      handler: createPullHandler(pwsPort),
      batchSize
    },
    push: {
      handler: createPushHandler({
        port: pwsPort,
        onWriteAccepted: createAckWriteBack({ rxCollection, writerId }),
        ...(writerId !== undefined && { writerId })
      }),
      batchSize
    }
  })
}
