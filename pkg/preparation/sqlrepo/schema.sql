-- enable foreign key constraints
PRAGMA foreign_keys = ON;
-- enable write ahead logging
PRAGMA journal_mode = WAL;

-- DROP TABLE IF EXISTS sources CASCADE;
-- DROP TABLE IF EXISTS spaces CASCADE;
-- DROP TABLE IF EXISTS space_sources;
-- DROP TABLE IF EXISTS uploads CASCADE;
-- DROP TABLE IF EXISTS scans CASCADE;
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
  state TEXT NOT NULL CHECK (state IN ('pending', 'started')),
  error_message TEXT,
  root_fs_entry_id BLOB,
  root_cid BLOB,
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (root_fs_entry_id) REFERENCES fs_entries(id),
  FOREIGN KEY (root_cid, space_did) REFERENCES nodes(cid, space_did)
) STRICT;

CREATE TABLE IF NOT EXISTS scans (
  id BLOB PRIMARY KEY,
  upload_id BLOB NOT NULL,
  root_id BLOB,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  state TEXT NOT NULL,
  error_message TEXT,
  FOREIGN KEY (upload_id) REFERENCES uploads(id),
  FOREIGN KEY (root_id) REFERENCES fs_entries(id)
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
  FOREIGN KEY (directory_id) REFERENCES fs_entries(id),
  FOREIGN KEY (child_id) REFERENCES fs_entries(id),
  PRIMARY KEY (directory_id, child_id)
) STRICT;

CREATE TABLE IF NOT EXISTS dag_scans (
  fs_entry_id BLOB NOT NULL PRIMARY KEY,
  upload_id BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  error_message TEXT,
  state TEXT NOT NULL,
  cid BLOB,
  space_did BLOB NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
  FOREIGN KEY (fs_entry_id) REFERENCES fs_entries(id),
  FOREIGN KEY (upload_id) REFERENCES uploads(id),
  FOREIGN KEY (cid, space_did) REFERENCES nodes(cid, space_did),
  FOREIGN KEY (space_did) REFERENCES spaces(did)
) STRICT;

CREATE TABLE IF NOT EXISTS nodes (
  cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  size INTEGER NOT NULL,
  ufsdata BLOB,
  path TEXT NOT NULL,
  source_id BLOB NOT NULL,
  OFFSET INTEGER NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  PRIMARY KEY (cid, space_did)
) STRICT;

CREATE TABLE IF NOT EXISTS links (
  name TEXT NOT NULL,
  t_size INTEGER NOT NULL,
  hash BLOB NOT NULL,
  parent_id BLOB NOT NULL,
  space_did BLOB NOT NULL,
  ordering INTEGER NOT NULL,
  FOREIGN KEY (parent_id, space_did) REFERENCES nodes(cid, space_did),
  FOREIGN KEY (hash, space_did) REFERENCES nodes(cid, space_did),
  FOREIGN KEY (space_did) REFERENCES spaces(did),
  PRIMARY KEY (name, t_size, hash, parent_id, space_did, ordering)
) STRICT;

-- The fact that a node has been assigned to a shard.
CREATE TABLE IF NOT EXISTS nodes_in_shards (
  -- Which node we're talking about
  node_cid BLOB NOT NULL,
  space_did BLOB NOT NULL,
  -- Which shard this node is in
  shard_id BLOB NOT NULL,
  -- Offset of the node in the shard
  -- If NULL, has not yet been calculated
  shard_offset INTEGER,
  FOREIGN KEY (node_cid, space_did) REFERENCES nodes(cid, space_did),
  FOREIGN KEY (shard_id) REFERENCES shards(id),
  PRIMARY KEY (node_cid, space_did, shard_id)
) STRICT;

CREATE TABLE IF NOT EXISTS shards (
  -- UUID identifying the shard locally
  id BLOB PRIMARY KEY,
  -- The upload this shard belongs to
  upload_id BLOB NOT NULL,
  -- The CID of the completed shard
  -- If NULL, has not yet been calculated (and maybe cannot be, if still
  -- accepting new nodes)
  cid BLOB,
  state TEXT NOT NULL
) STRICT;