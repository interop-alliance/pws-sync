# PWS Sync Roadmap (open items)

nextAvailableId: 33

Status as of 2026-09-05. Uses the formalized item structure shared across the
`@interop/*` repos.

Scope: open work items only. This document tracks the **remaining** items;
completed items move verbatim to [archived-roadmap.md](archived-roadmap.md) as
they land, so WS-N references keep resolving (CHANGELOG.md remains the record of
what landed).

## Item format

Each work item is a `### WS-N: Title` heading followed by a field block and free
prose context. Ids are permanent and never reused; the `nextAvailableId` line
above is the sole source of the next id, rewritten in the same edit that takes
it. Statuses: `todo`, `in-progress`, `draft` (no actionable done-state yet --
blocked externally or a parking record); `done` items move to
[archived-roadmap.md](archived-roadmap.md) once shipped. Full conventions live
in [AGENTS.md](AGENTS.md) under "Roadmap & Task Conventions".

---

### WS-32: Move the consumers to `@interop/pws-sync`

- status: todo
- priority: medium
- labels: packaging, was-react, freewallet
- touches:
  - was-react: `package.json`, `eslint.config.js`, `vite.config.ts`,
    ARCHITECTURE.md, AGENTS.md, README.md, and the `src/` and `test/` imports
  - freewallet: `package.json`, `tsconfig.app.json`, `vite.config.ts`,
    `eslint.config.js`, ARCHITECTURE.md, AGENTS.md, and the `src/` imports
- acceptance:
  - [ ] `@interop/pws-sync` is published to npm
  - [ ] was-react depends on `@interop/pws-sync`, with no `@interop/was-sync`
        import left in its `src/` or `test/`
  - [ ] freewallet depends on `@interop/pws-sync`, with no `@interop/was-sync`
        import left in its `src/` or `tests/`
  - [ ] each `touches:` entry is annotated with its follow-up item or what
        shipped there

Context: this package was renamed from `@interop/was-sync` to
`@interop/pws-sync`, but was-react and freewallet still import the old name.
They could not move with the rename, because the new name is not on npm yet and
both are still on `@interop/was-client`, while this release requires
`@interop/pws-client` as a peer. Until they move, the two consumers stay on
`@interop/was-sync` 0.11.0 and receive no driver changes.

Each consumer moves in the same change as its own `@interop/pws-client`
migration, since the driver's error matching depends on the `PwsSync*Error`
names that client raises.

---

### WS-31: Default the port's `spaceUrl` to the client's own

- status: todo
- priority: low
- labels: api, port, was-react
- touches:
  - pws-client (`createPwsSyncPort` takes the same `spaceUrl` beside `pws`; the
    same default applies there)
  - was-react (`SyncController` passes `remoteStore.spaceUrl` beside
    `remoteStore.was`; once the default lands, `WasRemoteStore.spaceUrl` can
    read `was.spaceUrl` instead of storing a second copy)
- acceptance:
  - [ ] `createSyncController`'s port accepts `spaceUrl` as optional; when
        absent, it reads `wasClient.spaceUrl` (set on a client built by
        `PwsClient.forSpace`)
  - [ ] A port with neither (`spaceUrl` absent and a client built from a base
        server URL) is refused at construction with an error naming both ways to
        supply the Space
  - [ ] A `spaceUrl` passed beside a `forSpace` client that disagrees with
        `wasClient.spaceUrl` is refused rather than silently preferred
  - [ ] The replication identifier and the per-collection port are built from
        the resolved value, so a consumer passing only the client gets the same
        identifier it gets today
  - [ ] Tests cover the three cases above; ARCHITECTURE.md's port description
        and the `createSyncController` header name the default

Context: since pws-client 0.100 a grants-only consumer builds its client with
`PwsClient.forSpace({ spaceUrl })`, and that client carries the Space URL as
`spaceUrl`. The port still requires `spaceUrl` as a separate member, so every
such consumer hands the controller two copies of one value (was-react's
`SyncController` passes `remoteStore.was` and `remoteStore.spaceUrl`), and the
consumer has to keep them consistent. A client built from a base server URL has
no single Space, so the member stays required there; the change is a default,
not a removal. Discovered during the was-react WR-98 port.

### WS-26: Record why the default resolver keeps the payload-stamp order under WSS-96

- status: todo
- priority: low
- labels: conflict, docs, was-96
- blocked-by: WS-23
- acceptance:
  - [ ] ARCHITECTURE.md's "Ownership heuristics" entry for the last-write-wins
        comparison and invariant 1 state that the resolver compares the
        payload's `(updatedAt, writerId)` because its local side never holds a
        server stamp, which is the case the spec's revised tie-break keeps
  - [ ] The entry names the condition under which that stops holding (a resolver
        that sees two stamped sides, for instance a future `keep-conflicts`
        sibling comparison) so the next reader can tell whether to reopen it

Context: WSS-96 invariant 12 rewrites the spec's client-side tie-break: stamped
records order by `(ms, counter, originId)`, and `(updatedAt, writerId)` is kept
for an unstamped local edit against a stamped remote. The default resolver's
rules 1 and 2 compare bodies, not revisions, so they survive the model, and its
LWW comparison reads the payload's `updatedAt` and `writerId` out of the
decrypted body (`lwwFieldsOf`), which is the unstamped-local case. Nothing
changes in code, but the spec sentence the comparison rests on is being
rewritten, so the reasoning needs to be on record here rather than inferred from
`@interop/social-core`.

### WS-29: Decide whether the default resolver detects a lagging replica by stamp comparison

- status: todo
- priority: low
- labels: conflict, was-96
- acceptance:
  - [ ] Either the default resolver compares the assumed and re-read primaries'
        write stamps when only `custom` differs and re-asserts the local state
        when the re-read one is older, with a test driving the `412` from a
        primary behind the assumed one, or ARCHITECTURE.md invariant 20 (a
        controller is bound to one server) records why the comparison was
        rejected and the consumer sequencing rule stays the answer

Context: After a failover to a replica behind the one the consumer left, a push
can draw a `412` whose re-read primary is an older revision than the assumed
primary this replica already holds. Rule 2 of the default resolver treats a
`custom`-only difference as a metadata edit committed on the server and adopts
the re-read `custom`, which in this case walks the row backwards. Under the
stamp model the write stamp `(updatedAt, updatedAtCounter, originId)` is a total
order shared by every replica, and both primaries carry one, so the resolver
could tell an older re-read primary from a newer one. The trade-off is that
re-asserting the local state then writes it onto the lagging replica with
`If-Match` on the older validator, which forks the two servers' histories for
their own replication to settle, where today the consumer is expected to fail
over only to a replica that has caught up. Invariant 20 records the current
assumption. discovered-from: WS-28.

### WS-20: A foreign state pulled during this replica's own push window is dropped

- status: todo
- priority: low
- labels: pull, push, correctness, rxdb
- acceptance:
  - [ ] Either the driver re-pulls a foreign state dropped inside the window, or
        the residual is documented in ARCHITECTURE.md invariant 18 with the
        conditions under which it occurs
  - [ ] An integration case lands another writer's `/meta` edit between this
        replica's content write and its write-back cycle and asserts the chosen
        outcome

Context: RxDB drops a pulled state for a row whose local state differs from its
assumed primary and moves the checkpoint past it. WS-17 makes that harmless for
this replica's own echo, since the ack now carries what the echo would bring. It
does nothing for another writer's change to the same row inside the window.
`/meta` is versioned on its own, so another writer can commit a metadata edit
right after this replica's content write without a `412` on either side. The
feed entry carries both, is pulled inside the window, and is dropped; this
replica's `custom` stays stale until the row next changes. Candidates: a
pull-side hold-back that trims the batch before an in-flight id so the entry is
re-pulled, or a cheaper rule that re-pulls only when the dropped entry's
validators differ from the ack's.

discovered-from: WS-17

Unchanged by WSS-96: its open point 2 keeps two stamp sets per Resource, so
`/meta` stays independently versioned and the race above stays reachable. The
"validators differ" rule would compare the nested `meta` stamp under WS-23.
