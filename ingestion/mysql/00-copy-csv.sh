#!/usr/bin/env bash
# Copy the read-only seed CSV into MySQL's writable secure_file_priv dir so the
# subsequent LOAD DATA INFILE (01-init-users.sql) can read it. We don't mount
# directly into /var/lib/mysql-files because the entrypoint chowns that dir.
set -e
cp /seed/users.csv /var/lib/mysql-files/users.csv
echo "[init] copied users.csv into secure_file_priv dir"
