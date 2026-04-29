# SQLite extension to replicate data from PostgreSQL

## Installation

Download the `postgresql` extension from the [releases page](https://github.com/litesql/postgresql/releases).

If you want to build it yourself, install Go 1.25+ and enable CGO:

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o postgresql.so
```

Use:
- `.so` on Linux
- `.dylib` on macOS
- `.dll` on Windows

## Basic usage

### Prepare PostgreSQL

1. Edit `postgresql.conf`
    - Set `wal_level = logical`
    - Increase `max_replication_slots` and `max_wal_senders` as needed

2. Edit `pg_hba.conf`
    - Add a replication entry for the subscriber IP, for example:

```text
host    replication     rep_user        subscriber_ip/32        md5
```

3. Create a replication user:

```sql
CREATE ROLE rep_user WITH REPLICATION LOGIN PASSWORD 'secret';
```

4. Create a publication:

```sql
CREATE PUBLICATION my_publication FOR TABLE table1, table2;
-- or for all tables
CREATE PUBLICATION my_publication FOR ALL TABLES;
```

5. Restart PostgreSQL.

### Prepare SQLite

Optional: convert PostgreSQL schema and data to SQLite:

```sh
go install github.com/litesql/postgresql/cmd/pg2sqlite@latest
pg2sqlite [postgresql_url] example.db
```

Load the extension:

```sh
sqlite3 example.db
.load ./postgresql
SELECT pg_info();
```

Start replication:

1. Create a slot if needed:

```sql
SELECT pg_create_slot(
  'postgres://rep_user:secret@127.0.0.1:5432/postgres',
  'my_slot'
);
```

2. Start replication by inserting into `pg_sub`:

```sql
INSERT INTO pg_sub(connect, slot, publication)
VALUES(
  'postgres://rep_user:secret@127.0.0.1:5432/postgres',
  'my_slot',
  'my_publication'
);
```

### Revert PostgreSQL Committed Transactions

The extension stores changes in `pg_history`. Undo completed PostgreSQL transactions in reverse order.

**Syntax:**

```
pg_undo(<dsn>, <slot>, <startSeq>, <filter>)
```

**Example:**

```sql
SELECT pg_undo(
  'postgres://postgres:secret@localhost:5432/postgres',
  'my_slot',
  0,
  ''
);
```

`startSeq` is the sequence number in `pg_history`. Use `0` to revert the most recent transaction only.

You can also specify a time duration to undo transactions from that point up to now:

```sql
SELECT pg_undo(
  'postgres://postgres:secret@localhost:5432/postgres',
  'my_slot',
  '5m',
  '' 
);
```

The fourth parameter filters which entities to revert. You can filter by table name or by table name with a specific column value.

**Syntax:**
```
tableName[.column=value]
```

**Examples:**

1. Revert all changes from the past 5 minutes on the `users` table:

```sql
SELECT pg_undo(
    'postgres://postgres:secret@localhost:5432/postgres',
    'my_slot',
    '5m',
    'users' 
);
```

2. Revert all changes from the past 5 minutes on the `users` table where `id=42`:

```sql
SELECT pg_undo(
    'postgres://postgres:secret@localhost:5432/postgres',
    'my_slot',
    '5m',
    'users.id=42' 
);
```

## Configure replication type

| Type | Description |
|------|-------------|
| 0 | Both: data and history are recorded in SQLite |
| 1 | DataOnly: only data changes are recorded |
| 2 | HistoryOnly: only history is stored |

Example: 
To skip history logging for a subscription, set `type` when inserting into `pg_sub`:

```sql
INSERT INTO pg_sub(connect, slot, publication, type)
VALUES(
    'postgres://rep_user:secret@127.0.0.1:5432/postgres',
    'my_slot',
    'my_publication',
    1
);
```

## Configuration

Configure replication parameters on the virtual table:

Param | Description | Default
--- | --- | ---
use_namespace | Keep schema/namespace instead of using the main database | false
position_tracker_table | Table for replication position checkpoints | pg_sub_stat
timeout | Timeout in milliseconds | 10000
logger | Log destination: `stdout`, `stderr`, or `file:/path/to/log.txt` | 

### Debugging

Enable logging with:

```sh
export SQLITE_PG_LOG=1
```

Logs will print to stderr.