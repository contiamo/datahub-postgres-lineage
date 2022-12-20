# Datahub Postgres View Lineage


A ingestion source to generate lineage for views in a Postgres database.


## Quick Start

First install [Poetry](https://python-poetry.org/docs/#installation) and [task](https://taskfile.dev) and initialize the project

```sh
task setup
```

Now, start a database

```sh
task start wait sample-view
```

Now run the ingestion to the console
```sh
task run
```

When it is successful, the output should include

```sh
Source (datahub_postgres_lineage.ingestion.PostgresLineageSource) report:
{'events_produced': '1',
 'events_produced_per_sec': '26',
 'event_ids': ['urn:li:dataset:(urn:li:dataPlatform:postgres,cool_db.public.emails,PROD)-upstreamLineage'],
 'warnings': {},
 'failures': {},
 'filtered': ['public.names'],
 'start_time': '2022-12-20 16:09:46.105046 (now).',
 'running_time': '0.04 seconds'}
```

## Configuration

| Key | Description | Default |
| --- | --- | --- |
| `username` | The username to connect to the database | '' |
| `password` | The password to connect to the database | '' |
| `host_port` | The host and port to connect to the database | '' |
| `database` | The database to connect to | '' |
| `database_alias` | Alias to apply to database when ingesting. | '' |
| `sqlalchemy_uri` | SQLAlchemy URI to connect to the database | '' |
| `scheme` | The SQLAlchemy scheme to use | `postgressql+psycopg2` |
| `schema_pattern` | | |
| `schema_pattern.allow`| Regexp pattern to match schemas to include | `.*` |
| `schema_pattern.deny` | Regexp pattern to match schemas to exclude, 'information_schema' and 'pg_catalog' are already excluded | '' |
| `view_pattern` | | |
| `view_pattern.allow` | Regexp pattern to match view names to include | `.*` |
| `view_pattern.deny` | Regexp pattern to match view names to exclude | '' |