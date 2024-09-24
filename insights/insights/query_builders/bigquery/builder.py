from google.cloud import bigquery
from ..sql_builder import ColumnFormatter, Functions, SQLQueryBuilder


class BigQueryColumnFormatter(ColumnFormatter):
    @classmethod
    def format_date(cls, format, column: str):
        # In BigQuery, date formatting is done using FORMAT_TIMESTAMP and FORMAT_DATE functions
        match format:
            case "Minute":
                return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', {column})"
            case "Hour":
                return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:00', {column})"
            case "Day" | "Day Short":
                return f"FORMAT_TIMESTAMP('%Y-%m-%d 00:00', {column})"
            case "Week":
                return f"FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC({column}, WEEK))"
            case "Month" | "Mon":
                return f"FORMAT_TIMESTAMP('%Y-%m-01', {column})"
            case "Year":
                return f"FORMAT_TIMESTAMP('%Y-01-01', {column})"
            case "Minute of Hour":
                return f"FORMAT_TIMESTAMP('%M', {column})"
            case "Hour of Day":
                return f"FORMAT_TIMESTAMP('%H:00', {column})"
            case "Day of Week":
                return f"FORMAT_TIMESTAMP('%A', {column})"
            case "Day of Month":
                return f"FORMAT_TIMESTAMP('%d', {column})"
            case "Day of Year":
                return f"FORMAT_TIMESTAMP('%j', {column})"
            case "Month of Year":
                return f"FORMAT_TIMESTAMP('%m', {column})"
            case "Quarter of Year":
                return f"EXTRACT(QUARTER FROM {column})"
            case "Quarter":
                return f"FORMAT_TIMESTAMP('%Y-%m-01', TIMESTAMP_TRUNC({column}, QUARTER))"
            case _:
                return f"FORMAT_TIMESTAMP('{format}', {column})"


class BigQueryQueryBuilder(SQLQueryBuilder):
    def __init__(self, client: bigquery.Client):
        super().__init__(client)
        self.client = client
        self.functions = Functions
        self.column_formatter = BigQueryColumnFormatter
