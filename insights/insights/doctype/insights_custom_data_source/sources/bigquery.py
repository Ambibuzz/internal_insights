# bigquery.py - Bridged file for Google BigQuery interaction

import re
import frappe
from google.cloud import bigquery
from google.oauth2 import service_account
from insights.insights.query_builders.bigquery.builder import BigQueryQueryBuilder

from .base_database import BaseDatabase
from .utils import create_insights_table

IGNORED_TABLES = ["__.*"]

BIGQUERY_TO_GENERIC_TYPES = {
    "INTEGER": "Integer",
    "FLOAT": "Decimal",
    "STRING": "Text",
    "BYTES": "String",
    "DATE": "Date",
    "TIMESTAMP": "Datetime",
    "BOOLEAN": "Boolean",
    "NUMERIC": "Decimal",
    "GEOGRAPHY": "Geography",
}


class BigQueryDatabase(BaseDatabase):
    def __init__(self, **kwargs):
        self.data_source = "BigQuery"
        self.project_id = kwargs.pop("project_id")
        self.service_account = kwargs.pop("service_account")
        try:
            self.client = bigquery.Client(
                project = self.project_id,
                credentials = service_account.Credentials.from_service_account_info(self.service_account)
            )
        except Exception as e:
            frappe.throw(frappe.throw(f"Error setting up BigQuery client: {e}"))
        
        datasets = self.client.list_datasets()
        self.project_tables = {}

        for dataset in datasets:
            dataset_id = dataset.dataset_id
            dataset_ref = self.client.dataset(dataset_id)
            tables = self.client.list_tables(dataset_ref)
            self.project_tables[dataset_id] = [table.table_id for table in tables]

        frappe.throw(str(self.project_tables))

        self.query_builder: BigQueryQueryBuilder = BigQueryQueryBuilder(self.client)

    def sync_tables(self, dataset_id, tables=None, force=False):
        self.table_factory.sync_tables(dataset_id, tables, force)

    def get_table_preview(self, dataset_id, table, limit=100):
        query = f"SELECT * FROM `{dataset_id}.{table}` LIMIT {limit}"
        results = self.execute_query(query)
        count_query = f"SELECT COUNT(*) FROM `{dataset_id}.{table}`"
        length = self.execute_query(count_query)[0][0]
        return {
            "data": results or [],
            "length": length or 0,
        }

    def get_table_columns(self, dataset_id, table):
        return self.table_factory.get_table_columns(dataset_id, table)

    def execute_query(self, query):
        job = self.client.query(query)
        return [dict(row.items()) for row in job.result()]

    def get_column_options(self, dataset_id, table, column, search_text=None, limit=50):
        query = f"SELECT DISTINCT {column} FROM `{dataset_id}.{table}` LIMIT {limit}"
        if search_text:
            query += f" WHERE {column} LIKE '%{search_text}%'"
        return self.execute_query(query)
