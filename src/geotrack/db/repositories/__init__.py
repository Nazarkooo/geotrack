"""Read and write paths behind the REST surface.

Every function takes an ``AsyncSession`` and returns plain pydantic models: geography
columns are projected with ``ST_X``/``ST_Y`` so nothing above this layer ever handles
WKB, and no lazy loading can fire outside the request that owns the session.
"""
