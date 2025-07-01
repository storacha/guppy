-- enable foreign key constraints
PRAGMA foreign_keys = ON;
-- enable write ahead logging
PRAGMA journal_mode = WAL;

-- DROP TABLE IF EXISTS sources CASCADE;
-- DROP TABLE IF EXISTS configurations CASCADE;
-- DROP TABLE IF EXISTS configuration_sources;
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
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS configurations (
  id BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  block_size INTEGER NOT NULL,
  links_per_node INTEGER NOT NULL,
  shard_size INTEGER NOT NULL,
  use_hamt_directory_size INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS configuration_sources (
  source_id BLOB NOT NULL,
  configuration_id BLOB NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (configuration_id) REFERENCES configurations(id),
  PRIMARY KEY (source_id, configuration_id)
) STRICT;

CREATE TABLE IF NOT EXISTS uploads (
  id BLOB PRIMARY KEY,
  configuration_id BLOB NOT NULL,
  source_id BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'scanning', 'generating_dag', 'sharding', 'uploading', 'completed', 'failed', 'cancelled')),
  error_message TEXT,
  FOREIGN KEY (configuration_id) REFERENCES configurations(id),
  FOREIGN KEY (source_id) REFERENCES sources(id)
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
  path TEXT NOT NULL,
  last_modified TEXT NOT NULL,
  MODE INTEGER NOT NULL,
  size INTEGER NOT NULL,
  CHECKSUM BLOB,
  FOREIGN KEY (source_id) REFERENCES sources(id)
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
  kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
  FOREIGN KEY (fs_entry_id) REFERENCES fs_entries(id),
  FOREIGN KEY (upload_id) REFERENCES uploads(id),
  FOREIGN KEY (cid) REFERENCES nodes(cid)
) STRICT;

CREATE TABLE IF NOT EXISTS nodes (
  cid BLOB PRIMARY KEY,
  size INTEGER NOT NULL,
  ufsddata BLOB NOT NULL,
  path TEXT NOT NULL,
  source_id BLOB NOT NULL,
  OFFSET INTEGER NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id)
) STRICT;

CREATE TABLE IF NOT EXISTS links (
  name TEXT NOT NULL,
  t_size INTEGER NOT NULL,
  hash BLOB NOT NULL,
  parent_id BLOB NOT NULL,
  ordering INTEGER NOT NULL,
  FOREIGN KEY (parent_id) REFERENCES nodes(cid),
  FOREIGN KEY (hash) REFERENCES nodes(cid),
  PRIMARY KEY (name, t_size, hash, parent_id, ordering)
) STRICT;