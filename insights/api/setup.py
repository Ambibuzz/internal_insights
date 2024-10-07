# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import json

import frappe

from insights.api.telemetry import track
from insights.setup.demo import setup as import_demo_data


@frappe.whitelist()
def setup_complete():
    return bool(frappe.get_single("Insights Settings").setup_complete)


@frappe.whitelist()
def update_erpnext_source_title(title):
    track("setup_erpnext_source")
    frappe.db.set_value("Insights Data Source", "Site DB", "title", title)


@frappe.whitelist()
def setup_sample_data(dataset):
    track("setup_sample_data")
    import_demo_data()
    import_demo_queries_and_dashboards()


def import_demo_queries_and_dashboards():
    demo_dashboard_exists = frappe.db.exists("Insights Dashboard", {"title": "eCommerce"})
    if demo_dashboard_exists:
        return
    try:
        setup_fixture_path = frappe.get_app_path("insights", "setup")
        with open(setup_fixture_path + "/demo_queries.json", "r") as f:
            queries = json.load(f)

        for query in queries:
            query_doc = frappe.new_doc("Insights Query")
            query_doc.update(query)
            query_doc.save(ignore_permissions=True)

        with open(setup_fixture_path + "/demo_dashboards.json", "r") as f:
            dashboards = json.load(f)

        for dashboard in dashboards:
            dashboard_doc = frappe.new_doc("Insights Dashboard")
            dashboard_doc.update(dashboard)
            dashboard_doc.save(ignore_permissions=True)
    except Exception as e:
        frappe.log_error("Failed to create Demo Queries and Dashboards")
        print(e)


@frappe.whitelist()
def submit_survey_responses(responses):
    track("submit_survey_responses")
    responses = frappe.parse_json(responses)

    try:
        responses = json.dumps(responses, default=str, indent=4)
        frappe.integrations.utils.make_post_request(
            "https://frappeinsights.com/api/method/insights.telemetry.submit_survey_responses",
            data={"response": responses},
        )
    except Exception:
        frappe.log_error(title="Error submitting survey responses")


def get_new_datasource(db):
    data_source = frappe.new_doc("Insights Data Source")
    if db.get("connection_string"):
        data_source.update(
            {
                "title": db.get("title"),
                "database_type": db.get("type"),
                "connection_string": db.get("connection_string"),
            }
        )
    if db.get("type") == "MariaDB" or db.get("type") == "PostgreSQL":
        data_source.update(
            {
                "database_type": db.get("type"),
                "database_name": db.get("name"),
                "title": db.get("title"),
                "host": db.get("host"),
                "port": db.get("port"),
                "username": db.get("username"),
                "password": db.get("password"),
                "use_ssl": db.get("useSSL"),
            }
        )
    if db.get("type") == "SQLite":
        data_source.update(
            {
                "database_type": db.get("type"),
                "title": db.get("title") or db.get("name"),
                "database_name": db.get("name") or frappe.scrub(db.get("title")),
            }
        )
    return data_source

def get_new_custom_datasource(db):
    data_source = frappe.new_doc("Insights Custom Data Source")
    if db.get("type") == "BigQuery" or db.get("type") == "RedShift" or db.get("type") == "S3":
        try:
            service_account = json.loads(db.get("service_account"))
        except json.JSONDecodeError as e:
            frappe.throw(
                title='Error',
                msg='Service account is not a valid JSON',
                exc=FileNotFoundError
            )
        data_source.update(
            {
                "database_type": db.get("type"),
                "title": db.get("title"),
                "project_id": db.get("project_id"),
                "service_account": service_account
            }
        )
    return data_source

@frappe.whitelist()
def test_database_connection(database):
    data_source = get_new_datasource(database)
    return data_source.test_connection(raise_exception=True)

@frappe.whitelist()
def test_custom_database_connection(database):
    data_source = get_new_custom_datasource(database)
    return data_source.test_connection(raise_exception=True)

@frappe.whitelist()
def add_database(database):
    track("add_data_source")
    data_source = get_new_datasource(database)
    data_source.save()
    data_source.enqueue_sync_tables()

@frappe.whitelist()
def add_custom_database(database):
    track("add_custom_data_source")
    data_source = get_new_custom_datasource(database)
    data_source.save()
    data_source.enqueue_sync_tables()


@frappe.whitelist()
def complete_setup():
    settings = frappe.get_single("Insights Settings")
    settings.setup_complete = 1
    settings.save()
    track("setup_complete")
