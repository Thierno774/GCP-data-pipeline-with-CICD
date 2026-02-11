import logging
import functions_framework
from flask import jsonify, make_response
from google.cloud import bigquery

# ==================================
# CONFIGURATION
# ==================================

PROJECT_ID = "data-engineering-486712"
DATASET_ID = "dataset_name"
TABLE_ID = "customer_table"
STAGING_TABLE = f"{TABLE_ID}_staging"
PRIMARY_KEY = "Customer_Id"
WATCH_FOLDER = "gcp_bq/"

# ==================================
# LOGGER
# ==================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))
    logger.addHandler(handler)


# ==================================
# CLOUD FUNCTION
# ==================================

@functions_framework.cloud_event
def gcs_to_bq_function(cloud_event):

    data = cloud_event.data
    bucket = data.get("bucket")
    name = data.get("name")

    logger.info("File received: %s", name)

    # --------------------------------------
    # 1️⃣ Filter file
    # --------------------------------------

    if not name.startswith(WATCH_FOLDER):
        logger.info("Skipping file outside folder")
        return make_response(("Ignored", 204))

    if not name.lower().endswith(".csv"):
        logger.info("Skipping non CSV file")
        return make_response(("Ignored", 204))

    uri = f"gs://{bucket}/{name}"

    # --------------------------------------
    # 2️⃣ BigQuery Client
    # --------------------------------------

    client = bigquery.Client(project=PROJECT_ID)

    staging_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}"
    main_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # --------------------------------------
    # 3️⃣ Load into staging table
    # --------------------------------------

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    try:
        load_job = client.load_table_from_uri(
            uri,
            staging_table_ref,
            job_config=job_config
        )
        load_job.result()
        logger.info("File loaded into staging successfully")
    except Exception as e:
        logger.error("Load error: %s", e, exc_info=True)
        return make_response((f"Load error: {e}", 500))

    # --------------------------------------
    # 4️⃣ Dynamic MERGE with change detection
    # --------------------------------------

    try:
        table = client.get_table(main_table_ref)

        columns = [
            field.name
            for field in table.schema
            if field.name != PRIMARY_KEY
        ]

        if not columns:
            logger.warning("No updatable columns found.")
            return make_response(("No columns to update", 200))

        # Generate UPDATE clause
        update_clause = ",\n        ".join(
            [f"T.{col} = S.{col}" for col in columns]
        )

        # Generate change detection condition (NULL-safe)
        change_condition = " OR\n            ".join(
            [f"T.{col} IS DISTINCT FROM S.{col}" for col in columns]
        )

        # INSERT clause
        insert_columns = ", ".join([PRIMARY_KEY] + columns)
        insert_values = ", ".join(
            [f"S.{PRIMARY_KEY}"] + [f"S.{col}" for col in columns]
        )

        merge_query = f"""
        MERGE `{main_table_ref}` T
        USING `{staging_table_ref}` S
        ON T.{PRIMARY_KEY} = S.{PRIMARY_KEY}

        WHEN MATCHED AND (
            {change_condition}
        )
        THEN UPDATE SET
            {update_clause}

        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        logger.info("Executing MERGE query:\n%s", merge_query)

        query_job = client.query(merge_query)
        query_job.result()

        logger.info("MERGE completed successfully")

    except Exception as e:
        logger.error("MERGE error: %s", e, exc_info=True)
        return make_response((f"Merge error: {e}", 500))

    # --------------------------------------
    # 5️⃣ Clean staging table
    # --------------------------------------

    try:
        client.query(
            f"TRUNCATE TABLE `{staging_table_ref}`"
        ).result()
        logger.info("Staging table truncated")
    except Exception as e:
        logger.warning("Could not truncate staging table: %s", e)

    return make_response(jsonify({
        "status": "success",
        "file_processed": name
    }), 200)
