# SQLite extension to replicate data from PostgreSQL

## Installation

Download **postgresql** extension from the [releases page](https://github.com/litesql/postgresql/releases).
Here's a great article that explains [how to install the SQLite extension.](https://antonz.org/install-sqlite-extension/)

### Compiling from source

- [Go 1.24+](https://go.dev) and CGO_ENABLED=1 is required.

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o postgresql.so
```

- Use .so extension for Linux, .dylib for MacOS and .dll for Windows

## Basic usage

### Prepare PostgreSQL

#### 1. Modify postgresql.conf:

- Set wal_level = logical.
- Adjust max_replication_slots and max_wal_senders according to the number of subscribers and replication slots needed.

#### 2. Modify pg_hba.conf:

Add an entry to allow the replication user to connect from the subscriber's IP address. For example:

```
host    replication     rep_user        subscriber_ip/32        md5
```

#### 3. Create a Replication User:

- Create a user with replication privileges:

```sql
CREATE ROLE rep_user WITH REPLICATION LOGIN PASSWORD 'secret';
```

#### 4. Create a Publication:

- Define which tables or all tables in a database should be replicated:

```sql
CREATE PUBLICATION my_publication FOR TABLE table1, table2;
-- or for all tables in the current database:
CREATE PUBLICATION my_publication FOR ALL TABLES;
```

#### 5. Restart PostgreSQL.

### Prepare SQLite

#### 1. Convert PostgreSQL databse to SQLite

```sh
go install github.com/litesql/postgresql/cmd/pg2sqlite@latest
```

```
pg2sqlite [postgresql_url] example.db
```

#### 2. Loading the extension

```sh
sqlite3 example.db

# Load the extension
.load ./postgresql

# check version (optional)
SELECT pg_info();
```

#### 3. Start replication to sqlite

- Create a slot (if necessary)

```sql
SELECT pg_create_slot('postgres://rep_user:secret@127.0.0.1:5432/postgres', 'my_slot');
```

- Insert data into pg_sub virtual table to start replication.

```sql
INSERT INTO temp.pg_sub(connect, slot, publication) VALUES('postgres://rep_user:secret@127.0.0.1:5432/postgres', 'my_slot', 'my_publication');
```

## Configuring

You can configure replication by passing parameters to the VIRTUAL TABLE.

| Param | Description | Default |
|-------|-------------|---------|
| use_namespace | Keep schema/namespace (otherwise always use main database) | false |
| position_tracker_table | Table to store replication position checkpoints | pg_pub_stat |
| timeout | Timeout in milliseconds | 10000 |
| logger | Log errors to "stdout, stderr or file:/path/to/log.txt" | |
