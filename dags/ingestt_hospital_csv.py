from __future__ import annotations

import csv
from pathlib import Path
from datetime import datetime

import psycopg2
from airflow.decorators import dag, task


DATA_DIR = Path("/opt/airflow/data")

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "hospital_db",
    "user": "hospital_user",
    "password": "hospital_password",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@dag(
    dag_id="ingest_hospital_csv",
    description="Ingest hospital CSV files into PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hospital", "csv", "postgres"],
)
def ingest_hospital_csv():

    @task
    def ingest_services():
        csv_path = DATA_DIR / "services.csv"

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        with get_connection() as connection:
            with connection.cursor() as cursor:
                with csv_path.open(mode="r", encoding="utf-8") as file:
                    reader = csv.DictReader(file)

                    for row in reader:
                        cursor.execute(
                            """
                            INSERT INTO services (
                                service_id,
                                service_name,
                                floor,
                                building
                            )
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (service_id)
                            DO UPDATE SET
                                service_name = EXCLUDED.service_name,
                                floor = EXCLUDED.floor,
                                building = EXCLUDED.building;
                            """,
                            (
                                int(row["service_id"]),
                                row["service_name"],
                                int(row["floor"]),
                                row["building"],
                            ),
                        )

    @task
    def ingest_patients():
        csv_path = DATA_DIR / "patients.csv"

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        with get_connection() as connection:
            with connection.cursor() as cursor:
                with csv_path.open(mode="r", encoding="utf-8") as file:
                    reader = csv.DictReader(file)

                    for row in reader:
                        cursor.execute(
                            """
                            INSERT INTO patients (
                                patient_id,
                                first_name,
                                last_name,
                                birth_date,
                                gender,
                                service_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (patient_id)
                            DO UPDATE SET
                                first_name = EXCLUDED.first_name,
                                last_name = EXCLUDED.last_name,
                                birth_date = EXCLUDED.birth_date,
                                gender = EXCLUDED.gender,
                                service_id = EXCLUDED.service_id;
                            """,
                            (
                                int(row["patient_id"]),
                                row["first_name"],
                                row["last_name"],
                                row["birth_date"],
                                row["gender"],
                                int(row["service_id"]),
                            ),
                        )

    services_task = ingest_services()
    patients_task = ingest_patients()

    services_task >> patients_task


ingest_hospital_csv()