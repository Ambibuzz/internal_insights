# bigquery.py - Bridged file for Google BigQuery interaction

import re
import frappe
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import column as Column
from sqlalchemy import inspect
from sqlalchemy import select as Select
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.engine.base import Connection
from sqlalchemy.dialects import registry
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData
from sqlalchemy.schema import *

from insights.insights.query_builders.postgresql.builder import PostgresQueryBuilder

from .base_database import BaseDatabase
from .utils import create_insights_table, get_sqlalchemy_engine

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
        registry.register('bigquery', 'pybigquery.sqlalchemy_bigquery', 'BigQueryDialect')

        self.engine = create_engine(
            'bigquery://',
            credentials_info=self.service_account
        )
        
        self.metadata = MetaData()

        table = Table('ambika_data.sales_register', self.metadata)
        
        frappe.throw(str(table.fullname))

        self.query_builder: BigQueryQueryBuilder = BigQueryQueryBuilder(self.client)

    def get_table(project_name: str, dataset_name: str, table_name: str) -> Table:
        table = Table(f'{project_name}.{dataset_name}.{table_name}', metadata, autoload_with=engine)
        return table

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
