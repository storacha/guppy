#!/bin/bash

db=$1

for table in $(sqlite3 "$db" "SELECT name from pragma_table_list WHERE schema = 'main' AND name != 'sqlite_schema';"); do
  projection=$(sqlite3 "$db" -cmd ".mode json" "PRAGMA table_info($table);" | jq -r 'map(if .type == "BLOB" then "hex(" + .name + ")" else .name end) | join(", ")')
  output=$(sqlite3 "$db" -cmd ".headers on" -cmd ".mode table" "SELECT $projection FROM $table;")

  if [ -n "$output" ]; then
    echo
    echo "$table"
    echo "$output"
  fi
done
