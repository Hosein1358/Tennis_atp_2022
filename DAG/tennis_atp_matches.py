

import os

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from airflow.operators.python_operator import PythonOperator

base_path = Path(__file__).parents[1]
data_file = os.path.join(
    base_path,
    "data",
    "atp_matches_2022.csv",
)
ge_root_dir = os.path.join(base_path, "config", "ge")

# In a production DAG, the global variables below should be stored as Airflow
# or Environment variables.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
gcp_bucket = os.environ.get("GCP_GCS_BUCKET")

bq_dataset = "tennise_matches_example"
bq_table = "atp_2022"

gcp_data_dest = "data/atp_matches_2022.csv"

with DAG(
    "tennis_atp_matches",
    description="Example DAG showcasing loading and data quality checking with BigQuery.",
    doc_md=__doc__,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=bq_dataset
    )

    
    """
    #### Create Temp Table for GE in BigQuery
    """
    create_temp_table = BigQueryCreateEmptyTableOperator(
        task_id="create_temp_table",
        dataset_id=bq_dataset,
        table_id=f"{bq_table}",
        schema_fields=[        
            {     "name": "tourney_id",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "tourney_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "surface",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "draw_size",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "tourney_level",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "tourney_date",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "match_num",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_id",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_seed",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_entry",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_hand",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_ht",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_ioc",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_age",     "mode": "NULLABLE",     "type": "FLOAT"   },
            {     "name": "loser_id",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_seed",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_entry",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_hand",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_ht",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_ioc",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_age",     "mode": "NULLABLE",     "type": "FLOAT"   },
            {     "name": "score",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "best_of",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "round",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "minutes",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_ace",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_df",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_svpt",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_1stIn",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_1stWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_2ndWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_SvGms",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_bpSaved",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_bpFaced",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_ace",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_df",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_svpt",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_1stIn",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_1stWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_2ndWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_SvGms",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_bpSaved",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_bpFaced",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_rank",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_rank_points",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_rank",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_rank_points",     "mode": "NULLABLE",     "type": "INTEGER"   }
        ],
    )

    """
    #### Upload tennis data to GCS
    Upload the test data to GCS so it can be transferred to BigQuery.
    """
    upload_tennis_data = LocalFilesystemToGCSOperator(
        task_id="upload_tennis_data",
        src=data_file,
        dst=gcp_data_dest,
        bucket=gcp_bucket,
    )

    
    """
    #### Transfer data from GCS to BigQuery
    Moves the data uploaded to GCS in the previous step to BigQuery, where
    Great Expectations can run a test suite against it.
    """
    transfer_tennis_data = GCSToBigQueryOperator(
        task_id="tennis_data_gcs_to_bigquery",
        bucket=gcp_bucket,
        source_objects=[gcp_data_dest],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_dataset, bq_table),
        schema_fields=[        
            {     "name": "tourney_id",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "tourney_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "surface",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "draw_size",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "tourney_level",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "tourney_date",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "match_num",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_id",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_seed",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_entry",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_hand",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_ht",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_ioc",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "winner_age",     "mode": "NULLABLE",     "type": "FLOAT"   },
            {     "name": "loser_id",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_seed",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_entry",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_name",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_hand",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_ht",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_ioc",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "loser_age",     "mode": "NULLABLE",     "type": "FLOAT"   },
            {     "name": "score",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "best_of",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "round",     "mode": "NULLABLE",     "type": "STRING"   },
            {     "name": "minutes",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_ace",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_df",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_svpt",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_1stIn",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_1stWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_2ndWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_SvGms",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_bpSaved",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "w_bpFaced",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_ace",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_df",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_svpt",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_1stIn",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_1stWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_2ndWon",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_SvGms",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_bpSaved",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "l_bpFaced",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_rank",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "winner_rank_points",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_rank",     "mode": "NULLABLE",     "type": "INTEGER"   },
            {     "name": "loser_rank_points",     "mode": "NULLABLE",     "type": "INTEGER"   }
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )

    

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    chain(
        begin,
        create_dataset,
        create_temp_table,
        upload_tennis_data,
        transfer_tennis_data,
        #[ge_bigquery_validation_pass, ge_bigquery_validation_fail],
        #delete_dataset,
        end,
    )
