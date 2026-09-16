-- The application migration also runs CREATE EXTENSION IF NOT EXISTS; creating it
-- here keeps a freshly initialised database usable for ad-hoc inspection.
CREATE EXTENSION IF NOT EXISTS postgis;
