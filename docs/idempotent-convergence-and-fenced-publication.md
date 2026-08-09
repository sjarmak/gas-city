# Idempotent convergence and fenced publication

Multi-store operations rarely need cross-store atomicity. They need a stable
identity, immutable intermediate results, and one strongly fenced step that
makes a result authoritative.

Consider an operation that creates an artifact, records provenance in a work
ledger, and marks the artifact current. A crash can happen between any two
steps. Retrying the whole operation should converge on one result instead of
accumulating `artifact-1`, `artifact-2`, and `artifact-3`.

## Requirements

### Recover by reading forward

A retry derives the same operation and artifact identities, then asks:

1. Is the result already published? Return it.
2. Does the deterministic artifact exist? Validate it and resume publication.
3. Does neither exist? Produce it, then continue.

This turns partial completion into an ordinary state, not an incident that
requires a compensating transaction.

### Separate creation from authority

Expensive intermediate effects should be immutable or idempotent.
Authoritative publication should be one small compare-and-set operation.

If generation 7 uploads an artifact after generation 8 has taken over,
generation 7 must lose at publication. Its immutable artifact can remain
unreferenced until garbage collection. Orphaned immutable data is cheaper than
stale authoritative state.

### Seal references structurally

A final record should reject references appended after sealing. Enforce that
in the store or destination, where every writer is subject to it. A check in one
client is a convention, not an invariant.

## Pattern 1: the publication pointer is the fence

```text
step 1  PUT  results/<work>/<generation>/<digest>
        create-if-absent; never overwrite

step 2  INSERT provenance with deterministic identity
        retry converges on the same row

step 3  CAS current_result
        expected "<generation>"
        next     "<generation>:<digest>"
```

Only the pointer flip is authoritative. Everything written before it is
orphanable.

A stale generation loses the compare-and-set as an ordinary outcome. It does
not retry the same stale publication until it wins, and it does not delete an
artifact another attempt may be validating.

The single-key restriction is useful. If publication requires several fields,
write satellite data first and flip one pointer last, or pack the authoritative
fields into the pointer value. Two independently updated publication keys
recreate the partial state this pattern is meant to remove.

## Pattern 2: provenance has deterministic identity

Append-only does not imply idempotent. A provenance table keyed by a random ID
duplicates rows when a retry lands after an uncertain commit.

Derive the row identity from the logical operation:

```text
provenance_id = UUIDv5(work_id | kind | artifact_ref | source)
```

An insert-or-ignore then converges. A unique constraint over the logical fields
is stronger because it makes the property structural, but deterministic IDs
provide the same retry shape when a schema change is not available.

## Pattern 3: references move from open to sealed

```text
        append reference
             |
             v
      +---- OPEN ---- seal with CAS ----> SEALED
      |                                      |
      +--------------------------------------+
                          append is rejected
```

The store must refuse an append against a sealed owner. Timestamps supplied by
the writer are not a fence. A client-side `if open { append() }` check is not a
fence either, because state can change between the check and write.

Useful implementations include:

- a `BEFORE INSERT` trigger that checks the owner's sealed state;
- a foreign-key-compatible reference table with a store-maintained generation;
- an API endpoint that validates a version and writes the reference in one
  transaction.

Verify refusal by reading the destination afterward. Some database clients and
shell wrappers can report success while an inner statement failed.

## Pattern 4: recovery converges instead of compensating

Compensation is reserved for external effects that are neither immutable nor
idempotent, such as creating a directory with a user-chosen name or provisioning
a mutable database.

For immutable artifacts and deterministic rows, recovery reads forward:

```text
published?
  yes -> return
  no  -> deterministic artifact exists?
           yes -> validate -> record provenance -> publish
           no  -> create   -> record provenance -> publish
```

No distributed lock is required for artifact creation. Competing attempts may
do duplicate computation, but only one publication pointer becomes current.

## Repository publication is the same pattern

Git already supports an authoritative compare-and-set:

```bash
git update-ref refs/heads/main <new-commit> <expected-old-commit>
```

Two publishers may prepare concurrently against the same base. One advances the
ref; the other receives a stale-base refusal and rebases or revalidates. The
exclusive section is the ref update, not fetch, build, test, or artifact upload.

This distinction changes throughput. A long publication pipeline does not need
to be a long serial section when the invariant lives in a small pointer update.

## Garbage collection follows authority

Once authoritative references cannot be added retroactively, an artifact that
no current pointer names is collectable after the retention window. Garbage
collection must still classify unreadable state as `UNKNOWN`; it may delete only
objects proven unreferenced.

## Failure tests

Exercise every crash boundary and race:

- crash after artifact creation but before provenance;
- crash after provenance but before publication;
- duplicate delivery of the same operation;
- two generations publish concurrently;
- stale generation attempts publication after takeover;
- append a reference after sealing;
- garbage collection encounters an unreadable owner record.

The invariants are:

- one authoritative result;
- no accepted stale-generation publication;
- retries converge on one provenance identity;
- sealed owners accept no new references;
- unknown ownership never becomes permission to delete.
