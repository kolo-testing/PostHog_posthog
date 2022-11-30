# This file contains all CREATE TABLE queries, used to sync and test schema
import re
from typing import Tuple

from posthog.clickhouse.dead_letter_queue import *
from posthog.clickhouse.plugin_log_entries import *
from posthog.models.app_metrics.sql import *
from posthog.models.cohort.sql import *
from posthog.models.event.sql import *
from posthog.models.group.sql import *
from posthog.models.ingestion_warnings.sql import (
    DISTRIBUTED_INGESTION_WARNINGS_TABLE_SQL,
    INGESTION_WARNINGS_DATA_TABLE_SQL,
)
from posthog.models.person.sql import *
from posthog.models.session_recording_event.sql import *

CREATE_MERGETREE_TABLE_QUERIES = (
    CREATE_COHORTPEOPLE_TABLE_SQL,
    PERSON_STATIC_COHORT_TABLE_SQL,
    DEAD_LETTER_QUEUE_TABLE_SQL,
    EVENTS_TABLE_SQL,
    GROUPS_TABLE_SQL,
    PERSONS_TABLE_SQL,
    PERSONS_DISTINCT_ID_TABLE_SQL,
    PERSON_DISTINCT_ID2_TABLE_SQL,
    PLUGIN_LOG_ENTRIES_TABLE_SQL,
    SESSION_RECORDING_EVENTS_TABLE_SQL,
    INGESTION_WARNINGS_DATA_TABLE_SQL,
    APP_METRICS_DATA_TABLE_SQL,
)
CREATE_DISTRIBUTED_TABLE_QUERIES = (
    WRITABLE_EVENTS_TABLE_SQL,
    DISTRIBUTED_EVENTS_TABLE_SQL,
    WRITABLE_SESSION_RECORDING_EVENTS_TABLE_SQL,
    DISTRIBUTED_SESSION_RECORDING_EVENTS_TABLE_SQL,
    DISTRIBUTED_INGESTION_WARNINGS_TABLE_SQL,
    DISTRIBUTED_APP_METRICS_TABLE_SQL,
)
CREATE_KAFKA_TABLE_QUERIES: Tuple[()] = ()
CREATE_MV_TABLE_QUERIES: Tuple[()] = ()

CREATE_TABLE_QUERIES = (
    CREATE_MERGETREE_TABLE_QUERIES
    + CREATE_DISTRIBUTED_TABLE_QUERIES
    + CREATE_KAFKA_TABLE_QUERIES
    + CREATE_MV_TABLE_QUERIES
)

build_query = lambda query: query if isinstance(query, str) else query()
get_table_name = lambda query: re.findall(r" ([a-z0-9_]+) ON CLUSTER", build_query(query))[0]
