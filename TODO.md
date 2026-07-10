# TODO

## Integration test with a real Postgres

Spin up a Docker container in CI, send a NOTIFY, verify the command runs.
This would exercise the full async pipeline end-to-end.

## Graceful shutdown

Handle Ctrl+C / SIGTERM cleanly:
- Stop accepting new notifications
- Let in-flight commands finish
- Close the DB connection
Right now the process just dies.