import re
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Optional, cast

import structlog
from constance import config
from django.conf import settings
from django.utils.timezone import now

from ee.clickhouse.client import sync_execute
from ee.clickhouse.sql.table_engines import MergeTreeEngine
from posthog.async_migrations.definition import (
    AsyncMigrationDefinition,
    AsyncMigrationOperation,
    AsyncMigrationOperationSQL,
)
from posthog.utils import flatten

logger = structlog.get_logger(__name__)

"""
Migration summary:

Schema change to migrate tables to support replication and more than
one shard.

This allows for higher scalability as more hosts can be added under ClickHouse.

The migration strategy:

    1. We have a list of tables that might need replacing below.
    2. For each one, we replace the current engine with the appropriate Replicated by:
        a. creating a new table with the right engine and identical schema
        b. temporarily stopping ingestion to the table by dropping the kafka table
        c. using `ALTER TABLE ATTACH/DROP PARTITIONS` to move data to the new table.
        d. rename tables
    3. Once all tables are updated, we create needed distributed tables and re-enable ingestion

We use ATTACH/DROP tables to do the table migration instead of a normal INSERT. This method allows
moving data without increasing disk usage between identical schemas.

`events` and `session_recording_events` require extra steps as they're also sharded:

    1. The new table should be named `sharded_TABLENAME`
    2. When re-enabling ingestion, we create `TABLENAME` and `writable_TABLENAME` tables
       which are responsible for distributed reads and writes.
    3. We re-create materialized views to write to `writable_TABLENAME`

Constraints:

    1. This migration relies on there being exactly one ClickHouse node when it's run.
    2. For person and events tables, the schema tries to preserve any materialized columns.
    3. This migration requires there to be no ongoing part merges while it's executing.
    4. This migration depends on 0002_events_sample_by. If it didn't, this could be a normal migration.
    5. This migration depends on the person_distinct_id2 async migration to have completed.
    6. We can't stop ingestion by dropping/detaching materialized view as we can't restore to the right (non-replicated) schema afterwards.
    7. Async migrations might fail _before_ a step executes and rollbacks need to account for that, which complicates renaming logic.
"""


@dataclass(frozen=True)
class TableMigrationData:
    name: str
    new_table_engine: MergeTreeEngine
    kafka_table_name: Optional[str]
    create_kafka_table: Optional[str]

    @property
    def renamed_table_name(self):
        return self.name

    @property
    def backup_table_name(self):
        return f"{self.name}_backup_0004_replicated_schema"

    @property
    def tmp_table_name(self):
        return f"{self.name}_tmp_0004_replicated_schema"


@dataclass(frozen=True)
class ShardedTableMigrationData(TableMigrationData):
    rename_to: str
    extra_tables: Dict[str, str]
    materialized_view_name: str
    create_materialized_view: str

    @property
    def renamed_table_name(self):
        return self.rename_to


class Migration(AsyncMigrationDefinition):

    description = "Replace tables with replicated counterparts"

    depends_on = "0003_fill_person_distinct_id2"

    def is_required(self):
        return "Distributed" not in cast(str, self.get_current_engine("events"))

    def precheck(self):
        if not settings.CLICKHOUSE_REPLICATION:
            return False, "CLICKHOUSE_REPLICATION env var needs to be set for this migration"

        number_of_nodes = self.get_number_of_nodes_in_cluster()
        if number_of_nodes > 1:
            return (
                False,
                f"ClickHouse cluster should only contain one node at the time of this migration, found {number_of_nodes}",
            )

        return True, None

    @cached_property
    def operations(self):
        TABLE_MIGRATION_OPERATIONS = list(
            flatten([list(self.replicated_table_operations(table)) for table in self.tables_to_migrate()])
        )
        RE_ENABLE_INGESTION_OPERATIONS = list(
            flatten([list(self.finalize_table_operations(table)) for table in self.tables_to_migrate()])
        )

        return [
            AsyncMigrationOperationSQL(sql="SYSTEM STOP MERGES", rollback="SYSTEM START MERGES"),
            AsyncMigrationOperation(
                fn=lambda _: setattr(config, "COMPUTE_MATERIALIZED_COLUMNS_ENABLED", False),
                rollback_fn=lambda _: setattr(config, "COMPUTE_MATERIALIZED_COLUMNS_ENABLED", True),
            ),
            *TABLE_MIGRATION_OPERATIONS,
            *RE_ENABLE_INGESTION_OPERATIONS,
            AsyncMigrationOperation(
                fn=lambda _: setattr(config, "COMPUTE_MATERIALIZED_COLUMNS_ENABLED", False),
                rollback_fn=lambda _: setattr(config, "COMPUTE_MATERIALIZED_COLUMNS_ENABLED", True),
            ),
            AsyncMigrationOperationSQL(sql="SYSTEM START MERGES", rollback="SYSTEM STOP MERGES",),
        ]

    def replicated_table_operations(self, table: TableMigrationData):
        yield AsyncMigrationOperationSQL(
            sql=f"""
            CREATE TABLE {table.tmp_table_name} AS {table.name}
            ENGINE = {self.get_new_engine(table)}
            """,
            rollback=f"DROP TABLE IF EXISTS {table.tmp_table_name}",
        )

        if table.kafka_table_name is not None:
            yield AsyncMigrationOperationSQL(
                sql=f"DROP TABLE IF EXISTS {table.kafka_table_name}", rollback=cast(str, table.create_kafka_table)
            )

        yield AsyncMigrationOperation(
            fn=lambda _: self.move_partitions(table.name, table.tmp_table_name),
            rollback_fn=lambda _: self.move_partitions(table.tmp_table_name, table.name),
        )

        yield AsyncMigrationOperation(
            fn=lambda _: self.rename_tables(
                [table.name, table.backup_table_name], [table.tmp_table_name, table.renamed_table_name],
            ),
            rollback_fn=lambda _: self.rename_tables(
                [table.renamed_table_name, table.tmp_table_name],
                [table.backup_table_name, table.name],
                verify_table_exists=table.backup_table_name,
            ),
        )

    def finalize_table_operations(self, table: TableMigrationData):
        # NOTE: Relies on IF NOT EXISTS on the query
        if isinstance(table, ShardedTableMigrationData):
            for table_name, create_table_query in table.extra_tables.items():
                yield AsyncMigrationOperationSQL(sql=create_table_query, rollback=f"DROP TABLE IF EXISTS {table_name}")

        if isinstance(table, ShardedTableMigrationData) and table.materialized_view_name is not None:
            yield AsyncMigrationOperationSQL(sql=f"DROP TABLE IF EXISTS {table.materialized_view_name}", rollback=None)

        if table.kafka_table_name is not None:
            yield AsyncMigrationOperationSQL(
                sql=cast(str, table.create_kafka_table), rollback=f"DROP TABLE IF EXISTS {table.kafka_table_name}"
            )

        if isinstance(table, ShardedTableMigrationData) and table.materialized_view_name is not None:
            yield AsyncMigrationOperationSQL(
                sql=table.create_materialized_view, rollback=f"DROP TABLE IF EXISTS {table.materialized_view_name}",
            )

    def get_current_engine(self, table_name: str) -> Optional[str]:
        result = sync_execute(
            "SELECT engine_full FROM system.tables WHERE database = %(database)s AND name = %(name)s",
            {"database": settings.CLICKHOUSE_DATABASE, "name": table_name},
        )

        return result[0][0] if len(result) > 0 else None

    def get_new_engine(self, table: TableMigrationData):
        """
        Returns new table engine statement for the table.

        Note that the engine statement also includes PARTITION BY, ORDER BY, SAMPLE BY and SETTINGS,
        so we use the current table as a base for that and only replace the MergeTree engine.

        Also note that we set a unique zookeeper path to avoid conflicts due to rollbacks as zookeeper
        does not clean up data after a `DROP TABLE`.
        """
        current_engine = cast(str, self.get_current_engine(table.name))

        table.new_table_engine.set_zookeeper_path_key(now().strftime("am0004_%Y%m%d%H%M%S"))
        # Remove the current engine from the string
        return re.sub(r".*MergeTree\([^\)]+\)", str(table.new_table_engine), current_engine)

    def move_partitions(self, from_table: str, to_table: str):
        """
        This step the new table with old tables data without using any extra space.

        `ATTACH PARTITION` uses hard links for the copy, so as long as the two datasets are equal everything is good.

        Constraints:
        1. Identical schemas between the two schemas
        2. Merges and ingestion are stopped on the table
        3. We can't use MOVE PARTITION due to validation errors due to differing table engines
        """

        running_merges = sync_execute(
            "SELECT count() FROM system.merges WHERE database = %(database)s AND table = %(table)s",
            {"database": settings.CLICKHOUSE_DATABASE, "table": from_table},
        )[0][0]

        assert (
            running_merges == 0
        ), f"No merges should be running on tables while partitions are being moved. table={from_table}"

        partitions = sync_execute(
            "SELECT DISTINCT partition FROM system.parts WHERE database = %(database)s AND table = %(table)s AND is_active",
            {"database": settings.CLICKHOUSE_DATABASE, "table": from_table},
        )

        for (partition,) in partitions:
            logger.info("Moving partitions between tables", from_table=from_table, to_table=to_table, id=partition)
            # :KLUDGE: Partition IDs are special and cannot be passed as arguments
            sync_execute(f"ALTER TABLE {to_table} ATTACH PARTITION {partition} FROM {from_table}")
            sync_execute(f"ALTER TABLE {from_table} DROP PARTITION {partition}")

    def rename_tables(self, rename_1, rename_2, verify_table_exists=None):
        # :KLUDGE: Due to how async migrations rollback works, we need to check whether backup table exists even if the rename failed
        #   in the first place
        if verify_table_exists and self.get_current_engine(verify_table_exists) is None:
            logger.info(
                "(Rollback) Source table doesn't exist, skipping renaming.", rename_1=rename_1, rename_2=rename_2
            )
            return

        return sync_execute(f"RENAME TABLE {rename_1[0]} TO {rename_1[1]}, {rename_2[0]} TO {rename_2[1]}")

    def get_number_of_nodes_in_cluster(self):
        return sync_execute(
            "SELECT count() FROM clusterAllReplicas(%(cluster)s, system, one)", {"cluster": settings.CLICKHOUSE_CLUSTER}
        )[0][0]

    def tables_to_migrate(self):
        from ee.clickhouse.sql.cohort import COHORTPEOPLE_TABLE_ENGINE
        from ee.clickhouse.sql.dead_letter_queue import (
            DEAD_LETTER_QUEUE_TABLE_ENGINE,
            KAFKA_DEAD_LETTER_QUEUE_TABLE_SQL,
        )
        from ee.clickhouse.sql.events import (
            DISTRIBUTED_EVENTS_TABLE_SQL,
            EVENTS_DATA_TABLE_ENGINE,
            EVENTS_TABLE_MV_SQL,
            KAFKA_EVENTS_TABLE_SQL,
            WRITABLE_EVENTS_TABLE_SQL,
        )
        from ee.clickhouse.sql.groups import GROUPS_TABLE_ENGINE, KAFKA_GROUPS_TABLE_SQL
        from ee.clickhouse.sql.person import (
            KAFKA_PERSON_DISTINCT_ID2_TABLE_SQL,
            KAFKA_PERSONS_TABLE_SQL,
            PERSON_DISTINCT_ID2_TABLE_ENGINE,
            PERSON_STATIC_COHORT_TABLE_ENGINE,
            PERSONS_TABLE_ENGINE,
        )
        from ee.clickhouse.sql.plugin_log_entries import (
            KAFKA_PLUGIN_LOG_ENTRIES_TABLE_SQL,
            PLUGIN_LOG_ENTRIES_TABLE_ENGINE,
        )
        from ee.clickhouse.sql.session_recording_events import (
            DISTRIBUTED_SESSION_RECORDING_EVENTS_TABLE_SQL,
            KAFKA_SESSION_RECORDING_EVENTS_TABLE_SQL,
            SESSION_RECORDING_EVENTS_DATA_TABLE_ENGINE,
            SESSION_RECORDING_EVENTS_TABLE_MV_SQL,
            WRITABLE_SESSION_RECORDING_EVENTS_TABLE_SQL,
        )

        return [
            ShardedTableMigrationData(
                name="events",
                new_table_engine=EVENTS_DATA_TABLE_ENGINE(),
                materialized_view_name="events_mv",
                rename_to="sharded_events",
                kafka_table_name="kafka_events",
                create_kafka_table=KAFKA_EVENTS_TABLE_SQL(),
                create_materialized_view=EVENTS_TABLE_MV_SQL(),
                extra_tables={"writable_events": WRITABLE_EVENTS_TABLE_SQL(), "events": DISTRIBUTED_EVENTS_TABLE_SQL()},
            ),
            ShardedTableMigrationData(
                name="session_recording_events",
                new_table_engine=SESSION_RECORDING_EVENTS_DATA_TABLE_ENGINE(),
                kafka_table_name="kafka_session_recording_events",
                create_kafka_table=KAFKA_SESSION_RECORDING_EVENTS_TABLE_SQL(),
                materialized_view_name="session_recording_events_mv",
                rename_to="sharded_session_recording_events",
                create_materialized_view=SESSION_RECORDING_EVENTS_TABLE_MV_SQL(),
                extra_tables={
                    "writable_session_recording_events": WRITABLE_SESSION_RECORDING_EVENTS_TABLE_SQL(),
                    "session_recording_events": DISTRIBUTED_SESSION_RECORDING_EVENTS_TABLE_SQL(),
                },
            ),
            TableMigrationData(
                name="events_dead_letter_queue",
                new_table_engine=DEAD_LETTER_QUEUE_TABLE_ENGINE(),
                kafka_table_name="kafka_events_dead_letter_queue",
                create_kafka_table=KAFKA_DEAD_LETTER_QUEUE_TABLE_SQL(),
            ),
            TableMigrationData(
                name="groups",
                new_table_engine=GROUPS_TABLE_ENGINE(),
                kafka_table_name="kafka_groups",
                create_kafka_table=KAFKA_GROUPS_TABLE_SQL(),
            ),
            TableMigrationData(
                name="person",
                new_table_engine=PERSONS_TABLE_ENGINE(),
                kafka_table_name="kafka_person",
                create_kafka_table=KAFKA_PERSONS_TABLE_SQL(),
            ),
            TableMigrationData(
                name="person_distinct_id2",
                new_table_engine=PERSON_DISTINCT_ID2_TABLE_ENGINE(),
                kafka_table_name="kafka_person_distinct_id2",
                create_kafka_table=KAFKA_PERSON_DISTINCT_ID2_TABLE_SQL(),
            ),
            TableMigrationData(
                name="plugin_log_entries",
                new_table_engine=PLUGIN_LOG_ENTRIES_TABLE_ENGINE(),
                kafka_table_name="kafka_plugin_log_entries",
                create_kafka_table=KAFKA_PLUGIN_LOG_ENTRIES_TABLE_SQL(),
            ),
            TableMigrationData(
                name="cohortpeople",
                new_table_engine=COHORTPEOPLE_TABLE_ENGINE(),
                kafka_table_name=None,
                create_kafka_table=None,
            ),
            TableMigrationData(
                name="person_static_cohort",
                new_table_engine=PERSON_STATIC_COHORT_TABLE_ENGINE(),
                kafka_table_name=None,
                create_kafka_table=None,
            ),
        ]
