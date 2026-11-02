# Docker

The recommended installation method on anything but Windows is to use docker compose.

Example docker compose file:
```yaml
services:
  mlm:
    image: ghcr.io/stirlingmouse/mlm:main 
    volumes:
      - ./config:/config # folder for the config file, place it in config/config.toml
      - ./data:/data # folder where mlm will keep a database
      - /mnt/Data:/mnt/Data # folder where your downloaded files and library can be accessed from
    environment:
      TZ: Europe/London # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
```

## Logs

MLM logs to the container's output (`docker logs`). These environment variables change what it logs and where:

- `MLM_LOG`: which messages are logged, as a [tracing filter](https://docs.rs/tracing-subscriber/0.3/tracing_subscriber/filter/struct.EnvFilter.html#directives), for example `mlm=info`. The default is `mlm=debug`.
- `MLM_LOG_DIR`: a folder to also write log files to, for example `/config/logs`. MLM starts a new file each day, named like `mlm.2026-11-01.log` after the date in UTC. Empty, the default in the Docker image, writes no files.
- `MLM_LOG_FILE`: a separate filter for the log files, so they can keep more detail than the container's output, for example `mlm=debug` while `MLM_LOG` is `mlm=info`. When it's unset or blank, the files use `MLM_LOG`. If it's set but none of it is a valid filter, the files use the default `mlm=debug`, as `MLM_LOG` does for the container's output.
- `MLM_LOG_MAX_FILES`: how many daily log files to keep, for example `14`. MLM deletes the oldest files named like its own (`mlm.YYYY-MM-DD.log`, with a real date) in `MLM_LOG_DIR` at startup and checks again every hour, but never the file for the current UTC day. Files dated later, as left by a clock that was ahead, aren't counted or deleted. When it's unset or empty, MLM keeps every file. A value that isn't a whole number of at least 1 stops MLM from starting while `MLM_LOG_DIR` isn't empty.

