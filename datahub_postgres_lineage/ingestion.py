from contextlib import contextmanager
import logging
from typing import Iterable, List, Optional

from pydantic import BaseModel

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, CursorResult

from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.api.source import TestableSource
from datahub.ingestion.api.workunit import MetadataWorkUnit

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)

# from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
#     DatasetProperties,
#     UpstreamLineage,
#     ViewProperties,
# )

from datahub.emitter import mce_builder
from datahub.emitter.mcp_builder import (
    mcps_from_mce,
    # wrap_aspect_as_workunit
)

logger: logging.Logger = logging.getLogger(__name__)


VIEW_LINEAGE_QUERY = """
WITH RECURSIVE view_deps AS (
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
UNION
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, source_ns.nspname as source_schema
, source_table.relname as source_table
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
INNER JOIN view_deps vd
    ON vd.dependent_schema = source_ns.nspname
    AND vd.dependent_view = source_table.relname
    AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
)


SELECT source_table, source_schema, dependent_view, dependent_schema
FROM view_deps
WHERE NOT (source_schema = 'information_schema' OR source_schema = 'pg_catalog')
ORDER BY source_schema, source_table;
"""


class ViewLineageEntry(BaseModel):
    # note that the order matches our query above
    # so pydantic is able to parse the tuple using parse_obj
    source_table: str
    source_schema: str
    dependent_view: str
    dependent_schema: str


class ViewLineage(BaseModel):
    lineage: List[ViewLineageEntry]


class PostgresLineageConfig(PostgresConfig):
    pass


@platform_name("Postgres")
@config_class(PostgresLineageConfig)
@support_status(SupportStatus.TESTING)
class PostgresLineageSource(
    SQLAlchemySource, StatefulIngestionSourceBase, TestableSource
):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "postgres")
        self.config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def test_connection(self):
        with self._get_connection() as conn:
            results = conn.execute("SELECT 1")
            row = results.fetchone()
            assert row is not None, "No rows returned from SELECT 1"
            assert row[0] == 1, "SELECT 1 returned unexpected value"

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # https://github.com/datahub-project/datahub/blob/b236d0958c046aa58a62eb44de0176a72c16ba8a/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_lineage.py#L166

        logger.info("Getting lineage for postgres")
        data: List[ViewLineageEntry] = []
        with self._get_connection() as conn:
            results: CursorResult = conn.execute(VIEW_LINEAGE_QUERY)

            if results.returns_rows is False:
                return None

            for row in results:
                logger.info(row)
                data.append(ViewLineageEntry.parse_obj(row))

        if len(data) == 0:
            return None

        # original reference was https://github.com/datahub-project/datahub/blob/9a1f78fc60f692ebbd57c0a8cbabe4bfde44376b/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_utils.py#L193
        # but the only addition seems to be a call to self.report
        # dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,{},PROD)".format(
        #     "test"
        # )
        # upstream_lineage: Optional[UpstreamLineage] = None
        # yield wrap_aspect_as_workunit(
        #     "dataset", dataset_urn, "upstreamLineage", upstream_lineage
        # )

        lineage_elements = {}
        # Loop over the lineages in the JSON data.
        for lineage in data:
            # Check if the dependent view + source schema already exists in the dictionary, if not create a new entry.
            # Use ':::::' to join as it is unlikely to be part of a schema or view name.
            key = ":::::".join([lineage.dependent_view, lineage.dependent_schema])
            if key not in lineage_elements:
                lineage_elements[key] = []

            # Append the source table to the list.
            lineage_elements[key].append(
                mce_builder.make_dataset_urn(
                    "postgres",
                    ".".join(
                        [
                            self.config.database,
                            lineage.source_schema,
                            lineage.source_table,
                        ]
                    ),
                    self.config.env,
                )
            )

        # Loop over the lineage elements dictionary.
        for key, source_tables in lineage_elements.items():
            # Split the key into dependent view and dependent schema
            dependent_view, dependent_schema = key.split(":::::")

            # Construct a lineage object.
            urn = mce_builder.make_dataset_urn(
                "postgres",
                ".".join([self.config.database, dependent_schema, dependent_view]),
                self.config.env,
            )
            lineage_mce = mce_builder.make_lineage_mce(
                source_tables,
                urn,
            )

            for item in mcps_from_mce(lineage_mce):
                wu = item.as_workunit()
                self.report.report_workunit(wu)
                yield wu

    @contextmanager
    def _get_connection(self) -> Connection:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        with engine.connect() as conn:
            yield conn
