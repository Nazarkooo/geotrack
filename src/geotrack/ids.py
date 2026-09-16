import uuid


def new_uuid() -> uuid.UUID:
    """Time-ordered UUIDv7: keeps B-tree inserts append-mostly while staying unguessable."""
    return uuid.uuid7()
