"""Redis Streams consumer tier: turns queued device reports into committed state.

A replica leases a set of shards, reads each shard's stream in batches, applies one
batch in a single PostGIS transaction and fans the committed result out over pub/sub.
"""
