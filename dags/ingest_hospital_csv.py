from __future__ import annotations

import csv
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from minio import Minio

import psycopg2
from airflow.decorators import dag, task


DATA_DIR = Path("/opt/airflow/data/input")

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "hospital_db",
    "user": "hospital_user",
    "password": "hospital_password",
}

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "secure": False,
}

MINIO_BUCKET = "hospital-data"
BRONZE_PREFIX = "bronze/patients"

COLUMN_MAPPING = {
    # French classic files
    "nom": "last_name",
    "prenom": "first_name",
    "age": "age",
    "pathologie": "pathology",
    "service": "service",

    # English file
    "last_name": "last_name",
    "first_name": "first_name",
    "years_old": "age",
    "disease": "pathology",
    "department": "service",

    # Semicolon file
    "nom_patient": "last_name",
    "prenom_patient": "first_name",
    "pathologie": "pathology",
    "service_destination": "service",

    # Dirty columns file
    "age_patient": "age",
    "diagnostic_pathologie": "pathology",
    "service_demande": "service",

    # Optional fields
    "telephone": "phone",
    "commentaire": "comment",
}


SERVICE_MAPPING = {
    "cardiologie": ("cardiologie", "Cardiologie"),
    "cardio": ("cardiologie", "Cardiologie"),

    "orthopedie": ("orthopedie", "Orthopédie"),
    "ortho": ("orthopedie", "Orthopédie"),

    "pediatrie": ("pediatrie", "Pédiatrie"),

    "gynecologie": ("gynecologie", "Gynécologie"),

    "neurologie": ("neurologie", "Neurologie"),
    "neuro": ("neurologie", "Neurologie"),

    "endocrinologie": ("endocrinologie", "Endocrinologie"),

    "urgences": ("urgences", "Urgences"),

    "chir_cardio": ("chirurgie_cardiovasculaire", "Chirurgie cardiovasculaire"),
    "chirurgie_cardiovasculaire": (
        "chirurgie_cardiovasculaire",
        "Chirurgie cardiovasculaire",
    ),

    "dermatologie": ("dermatologie", "Dermatologie"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_minio_client() -> Minio:
    return Minio(**MINIO_CONFIG)

def upload_file_to_bronze(client: Minio, file_path: Path) -> None:
    object_name = f"{BRONZE_PREFIX}/{file_path.name}"

    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        file_path=str(file_path),
        content_type="text/csv",
    )

    print(f"[bronze] Uploaded {file_path.name} to minio://{MINIO_BUCKET}/{object_name}")

def normalize_text(value: Any) -> str | None:
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    if value.upper() in {"N/A", "NA", "NULL", "NONE"}:
        return None

    return value


def remove_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_key(value: Any) -> str:
    value = normalize_text(value)

    if value is None:
        return ""

    value = remove_accents(value)
    value = value.lower()
    value = value.strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    value = value.strip("_")

    return value


def normalize_name(value: Any) -> str | None:
    value = normalize_text(value)

    if value is None:
        return None

    # Handles names such as "O'Connor" and "El Amrani" better than plain title().
    words = value.split(" ")
    cleaned_words = []

    for word in words:
        if word == "":
            continue

        apostrophe_parts = word.split("'")
        cleaned_apostrophe_parts = [
            part[:1].upper() + part[1:].lower()
            for part in apostrophe_parts
            if part
        ]

        cleaned_words.append("'".join(cleaned_apostrophe_parts))

    return " ".join(cleaned_words)


def normalize_age(value: Any) -> int | None:
    value = normalize_text(value)

    if value is None:
        return None

    normalized = remove_accents(value.lower())

    if "mois" in normalized:
        return 0

    match = re.search(r"-?\d+", normalized)

    if not match:
        return None

    age = int(match.group())

    if age < 0 or age > 120:
        return None

    return age


def normalize_optional_text(value: Any) -> str | None:
    return normalize_text(value)


def detect_delimiter(file_path: Path) -> str:
    sample = file_path.read_text(encoding="utf-8-sig")[:2048]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except csv.Error:
        return ";" if sample.count(";") > sample.count(",") else ","


def read_csv_rows(file_path: Path) -> list[dict[str, Any]]:
    delimiter = detect_delimiter(file_path)

    with file_path.open(mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        return list(reader)


def standardize_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    standardized = {
        "first_name": None,
        "last_name": None,
        "age": None,
        "pathology": None,
        "phone": None,
        "comment": None,
        "service": None,
    }

    for raw_column, raw_value in raw_row.items():
        normalized_column = normalize_key(raw_column)
        target_column = COLUMN_MAPPING.get(normalized_column)

        if target_column:
            standardized[target_column] = raw_value

    standardized["first_name"] = normalize_name(standardized["first_name"])
    standardized["last_name"] = normalize_name(standardized["last_name"])
    standardized["age"] = normalize_age(standardized["age"])
    standardized["pathology"] = normalize_optional_text(standardized["pathology"])
    standardized["phone"] = normalize_optional_text(standardized["phone"])
    standardized["comment"] = normalize_optional_text(standardized["comment"])

    return standardized


def normalize_service(value: Any) -> tuple[str, str] | None:
    service_key = normalize_key(value)

    if service_key == "":
        return None

    return SERVICE_MAPPING.get(service_key)


def validate_patient(row: dict[str, Any]) -> list[str]:
    errors = []

    if row["first_name"] is None:
        errors.append("Missing first_name")

    if row["last_name"] is None:
        errors.append("Missing last_name")

    if row["age"] is None:
        errors.append("Invalid age")

    if normalize_service(row["service"]) is None:
        errors.append("Unknown service")

    return errors


def insert_rejected_patient(
    cursor,
    source_file: str,
    raw_row: dict[str, Any],
    rejection_reason: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO rejected_patients (
            source_file,
            raw_data,
            rejection_reason
        )
        VALUES (%s, %s, %s);
        """,
        (
            source_file,
            json.dumps(raw_row, ensure_ascii=False),
            rejection_reason,
        ),
    )


def get_or_create_service(cursor, service_code: str, service_name: str) -> int:
    cursor.execute(
        """
        INSERT INTO services (
            service_code,
            service_name
        )
        VALUES (%s, %s)
        ON CONFLICT (service_code)
        DO UPDATE SET
            service_name = EXCLUDED.service_name
        RETURNING service_id;
        """,
        (
            service_code,
            service_name,
        ),
    )

    return cursor.fetchone()[0]


def patient_already_exists(cursor, patient: dict[str, Any], service_id: int) -> bool:
    cursor.execute(
        """
        SELECT patient_id
        FROM patients
        WHERE first_name = %s
          AND last_name = %s
          AND age = %s
          AND service_id = %s
        LIMIT 1;
        """,
        (
            patient["first_name"],
            patient["last_name"],
            patient["age"],
            service_id,
        ),
    )

    return cursor.fetchone() is not None


def insert_patient(
    cursor,
    patient: dict[str, Any],
    service_id: int,
    source_file: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO patients (
            first_name,
            last_name,
            age,
            pathology,
            phone,
            comment,
            service_id,
            source_file
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            patient["first_name"],
            patient["last_name"],
            patient["age"],
            patient["pathology"],
            patient["phone"],
            patient["comment"],
            service_id,
            source_file,
        ),
    )


@dag(
    dag_id="ingest_hospital_csv",
    description="Ingest and standardize hospital patient CSV files into PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hospital", "csv", "pipeline-as-code"],
)
def ingest_hospital_csv():

    @task
    def ingest_csv_files() -> dict[str, int]:
        csv_files = sorted(DATA_DIR.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

        minio_client = get_minio_client()

        for csv_file in csv_files:
            upload_file_to_bronze(minio_client, csv_file)

        inserted_patients = 0
        rejected_patients = 0
        duplicated_patients = 0
        inserted_or_updated_services = 0

        with get_connection() as connection:
            with connection.cursor() as cursor:
                for csv_file in csv_files:
                    rows = read_csv_rows(csv_file)

                    for raw_row in rows:
                        patient = standardize_row(raw_row)
                        errors = validate_patient(patient)

                        if errors:
                            insert_rejected_patient(
                                cursor=cursor,
                                source_file=csv_file.name,
                                raw_row=raw_row,
                                rejection_reason=", ".join(errors),
                            )
                            rejected_patients += 1
                            continue

                        service_code, service_name = normalize_service(patient["service"])

                        service_id = get_or_create_service(
                            cursor=cursor,
                            service_code=service_code,
                            service_name=service_name,
                        )
                        inserted_or_updated_services += 1

                        if patient_already_exists(cursor, patient, service_id):
                            duplicated_patients += 1
                            continue

                        insert_patient(
                            cursor=cursor,
                            patient=patient,
                            service_id=service_id,
                            source_file=csv_file.name,
                        )
                        inserted_patients += 1

        return {
            "files_processed": len(csv_files),
            "inserted_patients": inserted_patients,
            "rejected_patients": rejected_patients,
            "duplicated_patients": duplicated_patients,
            "inserted_or_updated_services": inserted_or_updated_services,
        }

    ingest_csv_files()


ingest_hospital_csv()