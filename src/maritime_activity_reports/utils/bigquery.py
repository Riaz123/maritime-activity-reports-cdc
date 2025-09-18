"""BigQuery utilities for executing queries and managing materialized views."""

import structlog
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = structlog.get_logger(__name__)


def get_bigquery_client(project_id: str) -> bigquery.Client:
    """Get BigQuery client for the specified project."""
    try:
        client = bigquery.Client(project=project_id)
        logger.info("BigQuery client created", project_id=project_id)
        return client
    except Exception as e:
        logger.error("Failed to create BigQuery client", project_id=project_id, error=str(e))
        raise


def run_bigquery_query(query: str, project_id: str, 
                      job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Dict]:
    """
    Execute a BigQuery query and return results.
    
    Args:
        query: SQL query to execute
        project_id: GCP project ID
        job_config: Optional job configuration
        
    Returns:
        List of dictionaries containing query results
    """
    client = get_bigquery_client(project_id)
    
    try:
        logger.info("Executing BigQuery query", project_id=project_id)
        logger.debug("Query details", query=query[:200] + "..." if len(query) > 200 else query)
        
        job = client.query(query, job_config=job_config)
        results = job.result()
        
        # Convert results to list of dictionaries
        rows = []
        for row in results:
            rows.append(dict(row))
        
        logger.info("BigQuery query completed successfully", 
                   project_id=project_id,
                   rows_returned=len(rows))
        
        return rows
        
    except Exception as e:
        logger.error("BigQuery query failed", 
                    project_id=project_id, 
                    error=str(e))
        raise


def create_dataset_if_not_exists(project_id: str, dataset_id: str, 
                                location: str = "europe-west2") -> None:
    """Create BigQuery dataset if it doesn't exist."""
    client = get_bigquery_client(project_id)
    
    dataset_ref = client.dataset(dataset_id, project=project_id)
    
    try:
        client.get_dataset(dataset_ref)
        logger.info("Dataset already exists", project_id=project_id, dataset_id=dataset_id)
    except NotFound:
        logger.info("Creating BigQuery dataset", 
                   project_id=project_id, 
                   dataset_id=dataset_id,
                   location=location)
        
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = "Maritime Activity Reports - Gold layer analytics"
        
        client.create_dataset(dataset)
        logger.info("Dataset created successfully", 
                   project_id=project_id, 
                   dataset_id=dataset_id)


def table_exists(project_id: str, dataset_id: str, table_id: str) -> bool:
    """Check if a BigQuery table exists."""
    client = get_bigquery_client(project_id)
    
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def get_table_info(project_id: str, dataset_id: str, table_id: str) -> Dict[str, Any]:
    """Get information about a BigQuery table."""
    client = get_bigquery_client(project_id)
    
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        
        return {
            "table_id": table.table_id,
            "project_id": table.project,
            "dataset_id": table.dataset_id,
            "created": table.created.isoformat() if table.created else None,
            "modified": table.modified.isoformat() if table.modified else None,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "table_type": table.table_type,
            "schema": [{"name": field.name, "type": field.field_type, "mode": field.mode} 
                      for field in table.schema],
            "location": table.location,
            "description": table.description
        }
        
    except NotFound:
        logger.error("Table not found", 
                    project_id=project_id, 
                    dataset_id=dataset_id, 
                    table_id=table_id)
        return {"error": "Table not found"}
    except Exception as e:
        logger.error("Failed to get table info", 
                    project_id=project_id, 
                    dataset_id=dataset_id, 
                    table_id=table_id,
                    error=str(e))
        return {"error": str(e)}


def create_materialized_view(project_id: str, dataset_id: str, view_id: str,
                           query: str, cluster_fields: Optional[List[str]] = None,
                           max_staleness_hours: int = 24) -> None:
    """Create a materialized view in BigQuery."""
    client = get_bigquery_client(project_id)
    
    try:
        view_ref = client.dataset(dataset_id, project=project_id).table(view_id)
        
        # Build the CREATE MATERIALIZED VIEW statement
        cluster_clause = ""
        if cluster_fields:
            cluster_clause = f"CLUSTER BY {', '.join(cluster_fields)}"
        
        full_query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{project_id}.{dataset_id}.{view_id}`
        {cluster_clause}
        OPTIONS (
            max_staleness = INTERVAL "{max_staleness_hours}" HOUR,
            allow_non_incremental_definition = true
        )
        AS
        {query}
        """
        
        logger.info("Creating materialized view", 
                   project_id=project_id,
                   dataset_id=dataset_id,
                   view_id=view_id)
        
        job = client.query(full_query)
        job.result()  # Wait for completion
        
        logger.info("Materialized view created successfully", view_id=view_id)
        
    except Exception as e:
        logger.error("Failed to create materialized view", 
                    view_id=view_id,
                    error=str(e))
        raise


def refresh_materialized_view(project_id: str, dataset_id: str, view_id: str) -> None:
    """Refresh a materialized view."""
    refresh_query = f"""
    CALL BQ.REFRESH_MATERIALIZED_VIEW('{project_id}.{dataset_id}.{view_id}')
    """
    
    try:
        run_bigquery_query(refresh_query, project_id)
        logger.info("Materialized view refreshed", view_id=view_id)
    except Exception as e:
        logger.error("Failed to refresh materialized view", 
                    view_id=view_id,
                    error=str(e))
        raise


def get_materialized_view_status(project_id: str, dataset_id: str) -> List[Dict[str, Any]]:
    """Get status of all materialized views in a dataset."""
    query = f"""
    SELECT
        table_name,
        table_type,
        creation_time,
        last_modified_time,
        row_count,
        size_bytes,
        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP() - last_modified_time) / 3600 as hours_since_refresh
    FROM
        `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE
        table_type = 'MATERIALIZED_VIEW'
    ORDER BY
        last_modified_time DESC
    """
    
    try:
        results = run_bigquery_query(query, project_id)
        logger.info("Retrieved materialized view status", 
                   project_id=project_id,
                   dataset_id=dataset_id,
                   view_count=len(results))
        return results
    except Exception as e:
        logger.error("Failed to get materialized view status", 
                    project_id=project_id,
                    dataset_id=dataset_id,
                    error=str(e))
        return []


def export_table_to_gcs(project_id: str, dataset_id: str, table_id: str,
                       gcs_uri: str, file_format: str = "PARQUET") -> None:
    """Export BigQuery table to Google Cloud Storage."""
    client = get_bigquery_client(project_id)
    
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        
        extract_job = client.extract_table(
            table_ref,
            gcs_uri,
            job_config=bigquery.ExtractJobConfig(
                destination_format=getattr(bigquery.DestinationFormat, file_format)
            )
        )
        
        extract_job.result()  # Wait for completion
        
        logger.info("Table exported to GCS successfully", 
                   table_id=table_id,
                   gcs_uri=gcs_uri,
                   file_format=file_format)
        
    except Exception as e:
        logger.error("Failed to export table to GCS", 
                    table_id=table_id,
                    gcs_uri=gcs_uri,
                    error=str(e))
        raise


def load_table_from_gcs(project_id: str, dataset_id: str, table_id: str,
                       gcs_uri: str, schema: Optional[List[bigquery.SchemaField]] = None,
                       file_format: str = "PARQUET", write_disposition: str = "WRITE_APPEND") -> None:
    """Load data from GCS into BigQuery table."""
    client = get_bigquery_client(project_id)
    
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        
        job_config = bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, file_format),
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
            schema=schema
        )
        
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        load_job.result()  # Wait for completion
        
        logger.info("Data loaded from GCS successfully", 
                   table_id=table_id,
                   gcs_uri=gcs_uri,
                   rows_loaded=load_job.output_rows)
        
    except Exception as e:
        logger.error("Failed to load data from GCS", 
                    table_id=table_id,
                    gcs_uri=gcs_uri,
                    error=str(e))
        raise


def get_query_statistics(job_id: str, project_id: str) -> Dict[str, Any]:
    """Get statistics for a completed query job."""
    client = get_bigquery_client(project_id)
    
    try:
        job = client.get_job(job_id, project=project_id)
        
        if job.job_type != "query":
            return {"error": "Job is not a query job"}
        
        stats = {
            "job_id": job.job_id,
            "state": job.state,
            "created": job.created.isoformat() if job.created else None,
            "started": job.started.isoformat() if job.started else None,
            "ended": job.ended.isoformat() if job.ended else None,
            "total_bytes_processed": job.total_bytes_processed,
            "total_bytes_billed": job.total_bytes_billed,
            "cache_hit": job.cache_hit,
            "slot_millis": job.slot_millis,
            "total_slot_ms": job.total_slot_ms,
            "num_dml_affected_rows": job.num_dml_affected_rows
        }
        
        if job.errors:
            stats["errors"] = [{"reason": error.reason, "message": error.message} 
                             for error in job.errors]
        
        return stats
        
    except Exception as e:
        logger.error("Failed to get query statistics", 
                    job_id=job_id,
                    project_id=project_id,
                    error=str(e))
        return {"error": str(e)}
