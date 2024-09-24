# bigquery.py - Bridged file for Google BigQuery interaction

import re
import frappe
from google.cloud import bigquery
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


class BigQueryTableFactory:
	"""Fetches tables and columns from BigQuery"""

	def __init__(self, data_source = "BigQuery") -> None:
		self.client = bigquery.Client()
		self.data_source = data_source

	def sync_tables(self, dataset_id, tables=None, force=False):
		for table in self.get_tables(dataset_id, table_names=tables):
			# When force is true, it will overwrite the existing columns & links
			create_insights_table(table, force=force)

	def get_tables(self, dataset_id, table_names=None):
		tables = []
		for table in self.get_db_tables(dataset_id, table_names):
			table.columns = self.get_table_columns(table.dataset, table.table)
			tables.append(table)
		return tables

	def get_db_tables(self, dataset_id, table_names=None):
		dataset_ref = self.client.dataset(dataset_id)
		tables = list(self.client.list_tables(dataset_ref))

		if table_names:
			tables = [table for table in tables if table.table_id in table_names]

		return [self.get_table(table) for table in tables if not self.should_ignore(table.table_id)]

	def should_ignore(self, table_name):
		return any(re.match(pattern, table_name) for pattern in IGNORED_TABLES)

	def get_table(self, table):
		return frappe._dict(
			{
				"table": table.table_id,
				"label": frappe.unscrub(table.table_id),
				"data_source": self.data_source,
			}
		)

	def get_all_columns(self, dataset_id):
		tables = self.get_db_tables(dataset_id)
		columns_by_table = {}
		for table in tables:
			columns = self.get_table_columns(dataset_id, table.table_id)
			for col in columns:
				columns_by_table.setdefault(table.table_id, []).append(
					self.get_column(col.name, col.field_type)
				)
		return columns_by_table

	def get_table_columns(self, dataset_id, table_id):
		table_ref = self.client.dataset(dataset_id).table(table_id)
		table = self.client.get_table(table_ref)  # API call to get table metadata
		return [
			self.get_column(schema_field.name, schema_field.field_type)
			for schema_field in table.schema
		]

	def get_column(self, column_name, column_type):
		return frappe._dict(
			{
				"column": column_name,
				"label": frappe.unscrub(column_name),
				"type": BIGQUERY_TO_GENERIC_TYPES.get(column_type, "String"),
			}
		)


class BigQueryDatabase(BaseDatabase):
	def __init__(self, **kwargs):
		self.data_source = "BigQuery"
		self.project_id = kwargs.pop("project_id")
		self.client = bigquery.Client(project=self.project_id)
		self.query_builder: BigQueryQueryBuilder = BigQueryQueryBuilder(self.client)
		self.table_factory: BigQueryTableFactory = BigQueryTableFactory(self.data_source)

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
