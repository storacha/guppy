-- enable foreign key constraints
PRAGMA foreign_keys = ON;
-- enable write ahead logging
PRAGMA journal_mode = WAL;

-- DROP TABLE IF EXISTS sources CASCADE;
-- DROP TABLE IF EXISTS spaces CASCADE;
-- DROP TABLE IF EXISTS space_sources;
-- DROP TABLE IF EXISTS uploads CASCADE;
-- DROP TABLE IF EXISTS fs_entries CASCADE;
-- DROP TABLE IF EXISTS directory_children;
-- DROP TABLE IF EXISTS dag_scans CASCADE;
-- DROP TABLE IF EXISTS nodes CASCADE;
-- DROP TABLE IF EXISTS links;
CREATE TABLE IF NOT EXISTS sources (
  id BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  path TEXT NOT NULL,
  connection_params BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS spaces (
  did BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  shard_size INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS space_sources (
  source_id BLOB NOT NULL,
  space_did BLOB NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  PRIMARY KEY (source_id, space_did)
) STRICT;

CREATE TABLE IF NOT EXISTS uploads (
  id BLOB PRIMARY KEY,
  space_did BLOB NOT NULL,
  source_id BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  root_fs_entry_id BLOB,
  root_cid BLOB,
  UNIQUE (space_did, source_id),
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (root_fs_entry_id) REFERENCES fs_entries(id) ON DELETE
  SET NULL,
    FOREIGN KEY (root_cid, space_did) REFERENCES nodes(cid, space_did)
) STRICT;

CREATE TABLE IF NOT EXISTS fs_entries (
  id BLOB PRIMARY KEY,
  source_id BLOB NOT NULL,
  space_did BLOB NOT NULL,
  path TEXT NOT NULL,
  last_modified INTEGER NOT NULL,
  MODE INTEGER NOT NULL,
  size INTEGER NOT NULL,
  CHECKSUM BLOB,
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (space_did) REFERENCES spaces(did)
) STRICT;

CREATE TABLE IF NOT EXISTS directory_children (
  directory_id BLOB NOT NULL,
  child_id BLOB NOT NULL,
  FOREIGN KEY (directory_id) REFERENCES fs_entries(id) ON DELETE CASCADE,
  FOREIGN KEY (child_id) REFERENCES fs_entries(id) ON DELETE CASCADE,
  PRIMARY KEY (directory_id, child_id)
) STRICT;

CREATE TABLE IF NOT EXISTS dag_scans (
  fs_entry_id BLOB NOT NULL PRIMARY KEY,
  upload_id BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  cid BLOB,
  space_did BLOB NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
  FOREIGN KEY (fs_entry_id) REFERENCES fs_entries(id) ON DELETE CASCADE,
  FOREIGN KEY (upload_id) REFERENCES uploads(id),
  FOREIGN KEY (cid, space_did) REFERENCES nodes(cid, space_did),
  FOREIGN KEY (space_did) REFERENCES spaces(did)
) STRICT;

CREATE TABLE IF NOT EXISTS nodes (
  cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  size INTEGER NOT NULL,
  ufsdata BLOB,
  path TEXT,
  source_id BLOB,
  OFFSET INTEGER,
  PRIMARY KEY(cid, space_did),
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  CONSTRAINT unix_fs_or_raw_node CHECK (
    (
      -- UnixFS node
      ufsdata IS NOT NULL
      AND path IS NULL
      AND OFFSET IS NULL
      AND source_id IS NULL
    )
    OR (
      -- Raw node
      path IS NOT NULL
      AND OFFSET IS NOT NULL
      AND source_id IS NOT NULL
      AND ufsdata IS NULL
    )
  )
) STRICT;

CREATE TABLE IF NOT EXISTS links (
  name TEXT NOT NULL,
  t_size INTEGER NOT NULL,
  hash BLOB NOT NULL,
  parent_id BLOB NOT NULL,
  space_did BLOB NOT NULL,
  ordering INTEGER NOT NULL,
  FOREIGN KEY (parent_id, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (hash, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  PRIMARY KEY (
    name,
    t_size,
    hash,
    parent_id,
    space_did,
    ordering
  )
) STRICT;

-- The fact that a node has been assigned to a shard.
CREATE TABLE IF NOT EXISTS nodes_in_shards (
  -- Which node we're talking about
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  -- Which shard this node is in
  shard_id BLOB NOT NULL,
  -- Offset of the node in the shard
  shard_offset INTEGER NOT NULL,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did) ON DELETE CASCADE,
  FOREIGN KEY (shard_id) REFERENCES shards(id),
  PRIMARY KEY (node_cid, space_did, shard_id)
) STRICT;

CREATE TABLE IF NOT EXISTS shards (
  -- UUID identifying the shard locally
  id BLOB PRIMARY KEY,
  -- The upload this shard belongs to
  upload_id BLOB NOT NULL,
  size INTEGER NOT NULL,
  -- The multihash digest of the completed shard
  -- If NULL, has not yet been calculated (and maybe cannot be, if still
  -- accepting new nodes)
  digest BLOB,
  state TEXT NOT NULL
) STRICT;