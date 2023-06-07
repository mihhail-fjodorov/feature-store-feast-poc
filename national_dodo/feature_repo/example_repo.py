from datetime import timedelta

import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, PushSource, RequestSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String


######### Entities #########
customer = Entity(name="customer", join_keys=["customer_uid"])

######## Data Sources #########
transaction_count_source = PostgreSQLSource(
    name="transaction_count_source",
    query="""
    SELECT 
      customer_uid::TEXT,
      event_time as event_timestamp,
      count_50_last_hour,
      count_200_last_hour,
      count_500_last_hour,
      created_at
    FROM transaction_minutly_stats
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

transaction_amount_source = PostgreSQLSource(
    name="transaction_amount_source",
    query="""
    SELECT 
      customer_uid::TEXT,
      event_time as event_timestamp,
      sum_50_last_hour,
      sum_200_last_hour,
      sum_500_last_hour,
      created_at
    FROM transaction_minutly_stats
    """,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

###### Feature Views #######
transaction_hourly_counts = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="transaction_hourly_counts",
    entities=[customer],
    ttl=timedelta(hours=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="count_50_last_hour", dtype=Int64),
        Field(name="count_200_last_hour", dtype=Int64),
        Field(name="count_500_last_hour", dtype=Int64),
    ],
    online=True,
    source=transaction_count_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "fraud"},
)

transaction_hourly_sum = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="transaction_hourly_sum",
    entities=[customer],
    ttl=timedelta(hours=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="sum_50_last_hour", dtype=Float32),
        Field(name="sum_200_last_hour", dtype=Float32),
        Field(name="sum_500_last_hour", dtype=Float32),
    ],
    online=True,
    source=transaction_amount_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "fraud"},
)

####### On Demand Features Views #######
@on_demand_feature_view(
    sources=[
        transaction_hourly_counts,
        transaction_hourly_sum,
    ],
    schema=[
        Field(name="avt_sum_50", dtype=Float64),
        Field(name="avt_sum_200", dtype=Float64),
        Field(name="avt_sum_500", dtype=Float64),

    ]
)
def get_sum_to_count_ratio(inputs: pd.DataFrame) -> pd.DataFrame:
    output = pd.DataFrame()
    output["avt_sum_50"] = inputs["sum_50_last_hour"] / inputs["count_50_last_hour"]
    output["avt_sum_200"] = inputs["sum_200_last_hour"] / inputs["count_200_last_hour"]
    output["avt_sum_500"] = inputs["sum_500_last_hour"] / inputs["count_500_last_hour"]
    return output


####### Feaaure Services ########
transaction_stats_service_v1 = FeatureService(
    name="transaction_stats_service_v1",
    features=[
        transaction_hourly_counts,  # Selects all features from the feature view
    ],
)

transaction_stats_service_v2 = FeatureService(
    name="transaction_stats_service_v2",
    features=[
        transaction_hourly_counts[["count_50_last_hour"]],  # Selects all features from the feature view
        transaction_hourly_sum,
    ],
)

transaction_stats_service_v3 = FeatureService(
    name="transaction_stats_service_v3",
    features=[
        get_sum_to_count_ratio,
        transaction_hourly_counts,
        transaction_hourly_sum,
    ],
)

