# pg-dispatch

Listens to a PostgreSQL notification channel and executes a command for each
notification received. The notification payload, if any, is sent to the
command's standard input.

## Installation

```sh
$ git clone https://github.com/common-group/pg-dispatcher.git
$ cd pg-dispatcher
$ cargo build --release
```

## Usage

```
$ pg-dispatcher --help

Listens to a PostgreSQL notification channel and executes a command for each notification.

Usage: pg-dispatcher [OPTIONS] --db-uri <DB_URI> --channel <CHANNEL> --exec <EXEC>

Options:
      --db-uri <DB_URI>      Database connection string (e.g. postgres://user@host:port/dbname)
      --channel <CHANNEL>    PostgreSQL channel to LISTEN on
      --exec <EXEC>          Command to execute when a notification arrives. Arguments may be
                             included, e.g. `sh script.sh`
      --workers <WORKERS>    Maximum number of worker threads to spawn (default: 4) [default: 4]
  -h, --help                 Print help
  -V, --version              Print version
```

## Examples

### Dispatching a command without arguments

The example below listens to a PostgreSQL channel named `test_channel` and
executes `cat` for each notification, using up to 100 worker threads.

*(Note that `cat` reads from standard input when no file is specified.)*

```sh
$ ./target/release/pg-dispatcher                       \
      --db-uri='postgres://postgres@localhost/postgres'  \
      --channel="test_channel"                           \
      --exec=cat                                         \
      --workers=100
```

Then, in a PostgreSQL session:

```sql
postgres=# NOTIFY test_channel, 'hello from postgres';
```

Output:

```
[pg-dispatch] Listening to channel: "test_channel".
[worker-0] command succeeded with status code 0.
[cat-0] hello from postgres
```

### Dispatching a command with arguments

Arguments can be included in the `--exec` string:

```sh
$ ./target/release/pg-dispatcher                         \
      --db-uri='postgres://postgres@localhost/postgres'  \
      --channel="test_channel"                           \
      --exec="sh some-script.sh"                         \
      --workers=100
```

Where `some-script.sh`:

```sh
#!/bin/sh
PAYLOAD=$(cat) # read from stdin
echo "The payload was: $PAYLOAD!"
```

Output after a notification:

```
[pg-dispatch] Listening to channel: "test_channel".
[worker-0] command succeeded with status code 0.
[sh-0] The payload was: hello from postgres!
```