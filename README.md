# pg-dispatcher

Listens to a `PostgreSQL` notification channel and executes a command for each
notification received. The notification payload is sent to the command's
standard input.

## Installation

```sh
cargo install pg-dispatcher
```

or from source:

```sh
git clone https://github.com/common-group/pg-dispatcher.git
cd pg-dispatcher
cargo build --release
```

## Usage

```
$ pg-dispatcher --help

Listens to a PostgreSQL notification channel and executes a command for each notification

Usage: pg-dispatcher [OPTIONS] --db-uri <DB_URI> --channel <CHANNEL> --exec <EXEC>

Options:
      --db-uri <DB_URI>      Database connection string (e.g. postgres://user@host:port/dbname)
      --channel <CHANNEL>    PostgreSQL channel to LISTEN on
      --exec <EXEC>          Command to execute when a notification arrives. Arguments may be
                             included, e.g. `sh script.sh`
      --workers <WORKERS>    Maximum number of worker threads to spawn [default: 4]
  -h, --help                 Print help
  -V, --version              Print version
```

### Logging

The binary uses [`tracing`](https://docs.rs/tracing) with `RUST_LOG` for log
level control:

```sh
RUST_LOG=info pg-dispatcher --db-uri ... --channel events --exec cat
RUST_LOG=debug pg-dispatcher --db-uri ... --channel events --exec cat
```

## Examples

### Dispatching a command without arguments

Listens to channel `test_channel` and runs `cat` for each notification:

```sh
pg-dispatcher \
    --db-uri='postgres://postgres@localhost/postgres' \
    --channel=test_channel \
    --exec=cat \
    --workers=100
```

Then in a `PostgreSQL` session:

```sql
NOTIFY test_channel, 'hello from postgres';
```

Output:

```
[pg-dispatch] Listening to channel: "test_channel".
[worker-0] command succeeded with status code 0.
[cat-0] hello from postgres
```

### Dispatching a command with arguments

```sh
pg-dispatcher \
    --db-uri='postgres://postgres@localhost/postgres' \
    --channel=test_channel \
    --exec="sh some-script.sh" \
    --workers=100
```

Where `some-script.sh`:

```sh
#!/bin/sh
PAYLOAD=$(cat) # read from stdin
echo "The payload was: $PAYLOAD!"
```

## Library

The crate can also be used as a library. The core traits
[`NotificationSource`](https://docs.rs/pg-dispatcher/latest/pg_dispatcher/trait.NotificationSource.html)
and
[`CommandRunner`](https://docs.rs/pg-dispatcher/latest/pg_dispatcher/trait.CommandRunner.html)
enable dependency inversion — plug in your own source or runner for testing.

```rust
use pg_dispatcher::{Dispatcher, PgNotificationSource, ThreadPool, CommandSpec};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = PgNotificationSource::connect(
        "postgres://user@localhost/db",
        "events",
    )?;

    let command = CommandSpec::new("cat");
    let runner = ThreadPool::new(4, command);
    let dispatcher = Dispatcher::new(runner);

    let mut source = source;
    dispatcher.run(&mut source);

    Ok(())
}
```

## License

GPL-3.0