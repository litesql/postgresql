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

### Loading the extension

```sh
sqlite3

# Load the extension
.load ./postgresql

# check version (optional)
SELECT pg_info();
```

### Subscribe

```sh
INSERT INTO temp.pg(connect, slot, publication, plugin) VALUES('postgres://replication_user:secret@127.0.0.1:5432/postgres?replication=database', 'my_slot', 'my_publication', 'pgoutput');
```