# Datahub Postgres View Lineage


A ingestion source to generate lineage for views in a Postgres database.


## Quick Start

First install [Poetry](https://python-poetry.org/docs/#installation)

Now, start a database

```sh
docker compose up -f examples/docker-compose.yaml -d
```

Create some tables and views in your database using psql

```sh
psql -U normal_user -d cool_db -h localhost -p 6432 -c "create table if not exists users ( id uuid primary key, name text, email text, age int);"
psql -U normal_user -d cool_db -h localhost -p 6432 -c "create view names as select distinct(name) from users;"
```

Now run the ingestion to the console
```sh
poetry install
poetry run datahub ingest -c examples/recipe.yaml
```

When it is successful, the output should include

```sh
Source (datahub_postgres_lineage.ingestion.PostgresLineageSource) report:
{'events_produced': '1',
 'events_produced_per_sec': '1',
 'event_ids': ['urn:li:dataset:(urn:li:dataPlatform:postgres,cool_db.public.names,PROD)-upstreamLineage'],
 'warnings': {},
 'failures': {},
 'soft_deleted_stale_entities': [],
 'tables_scanned': '0',
 'views_scanned': '0',
 'entities_profiled': '0',
 'filtered': [],
 'start_time': '2022-12-16 18:17:57.889983 (now).',
 'running_time': '0.59 seconds'}
```