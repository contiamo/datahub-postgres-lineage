import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport, TestableSource
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.postgres import PostgresConfig
from datahub.ingestion.source.sql.sql_common import make_sqlalchemy_uri
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.utilities.lossy_collections import LossyList
from pydantic import BaseModel, Field, SecretStr
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine.cursor import CursorResult

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


@dataclass
class LineageSourceReport(SourceReport):
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


class PostgresLineageConfig(StatefulIngestionConfigBase):
    options: dict = {}

    username: Optional[str] = Field(default=None, description="username")
    password: Optional[SecretStr] = Field(
        default=None, exclude=True, description="password"
    )
    host_port: str = Field(description="host URL")
    database: Optional[str] = Field(default=None, description="database name")
    database_alias: Optional[str] = Field(
        default=None, description="Alias to apply to database when ingesting."
    )
    sqlalchemy_uri: Optional[str] = Field(
        default=None,
        description="URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
    )

    # defaults
    scheme = Field(default="postgresql+psycopg2", description="database scheme")
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Both 'information_schema' and 'pg_catalog' are excluded by default.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for views to filter in ingestion.",
    )

    def get_identifier(self, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular

    def get_sql_alchemy_url(self, uri_opts: Optional[Dict[str, Any]] = None) -> str:
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and schema or connect_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,  # type: ignore
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,  # type: ignore
            self.database,
            uri_opts=uri_opts,
        )


@platform_name("PostgresLineage")
@config_class(PostgresLineageConfig)
@support_status(SupportStatus.TESTING)
class PostgresLineageSource(StatefulIngestionSourceBase, TestableSource):
    config: PostgresLineageConfig  # type: ignore
    report: LineageSourceReport  # type: ignore

    def __init__(self, config: PostgresLineageConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.platform = "postgres"
        self.config = config
        self.report = LineageSourceReport()

    ### Start required abstract class methods
    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresLineageConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        config_dict = self.config.dict()
        host_port = config_dict.get("host_port", "no_host_port")
        database = config_dict.get("database", "no_database")
        return f"{self.platform}_{host_port}_{database}"

    def get_report(self):
        return self.report

    def close(self) -> None:
        super().close()

    ### End required abstract class methods

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

        lineage_elements: Dict[str, List[str]] = {}
        # Loop over the lineages in the JSON data.
        for lineage in data:
            if not self.config.view_pattern.allowed(lineage.dependent_view):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            if not self.config.schema_pattern.allowed(lineage.dependent_schema):
                self.report.report_dropped(
                    f"{lineage.dependent_schema}.{lineage.dependent_view}"
                )
                continue

            # Check if the dependent view + source schema already exists in the dictionary, if not create a new entry.
            # Use ':::::' to join as it is unlikely to be part of a schema or view name.
            key = ":::::".join([lineage.dependent_view, lineage.dependent_schema])
            if key not in lineage_elements:
                lineage_elements[key] = []

            # Append the source table to the list.
            lineage_elements[key].append(
                mce_builder.make_dataset_urn(
                    "postgres",
                    self.config.get_identifier(
                        lineage.source_schema,
                        lineage.source_table,
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
                self.config.get_identifier(
                    dependent_schema,
                    dependent_view,
                ),
                self.config.env,
            )

            # use the mce_builder to ensure that the change proposal inherits
            # the correct defaults for auditHeader and systemMetadata
            lineage_mce = mce_builder.make_lineage_mce(
                source_tables,
                urn,
            )

            for item in mcps_from_mce(lineage_mce):
                wu = item.as_workunit()
                self.report.report_workunit(wu)
                yield wu

    @contextmanager
    def _get_connection(self) -> Iterator[Connection]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        with engine.connect() as conn:
            yield conn
