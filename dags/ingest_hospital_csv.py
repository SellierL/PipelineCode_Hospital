from __future__ import annotations

import io
import json
import re
import unicodedata
from datetime import datetime
from typing import Any

import pandas as pd
import psycopg2
from airflow.sdk import dag, task
from minio import Minio


# =========================
# Configuration
# =========================

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "hospital-data"

# À adapter selon l’endroit où tu as déposé les fichiers dans MinIO.
MINIO_PREFIX = ""

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "hospital_db",
    "user": "hospital_user",
    "password": "hospital_password",
}


# =========================
# Mappings de standardisation
# =========================

COLUMN_MAPPING = {
    # Fichiers FR classiques
    "nom": "last_name",
    "prenom": "first_name",
    "age": "age",
    "pathologie": "pathology",
    "service": "service",

    # Fichier anglais
    "last_name": "last_name",
    "first_name": "first_name",
    "years_old": "age",
    "disease": "pathology",
    "department": "service",

    # Fichier avec séparateur ; et colonnes majuscules
    "nom_patient": "last_name",
    "prenom_patient": "first_name",
    "service_destination": "service",

    # Fichier colonnes sales
    "age_patient": "age",
    "diagnostic_pathologie": "pathology",
    "service_demande": "service",

    # Champs optionnels
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


# =========================
# Connexions
# =========================

def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def get_postgres_connection():
    return psycopg2.connect(**DB_CONFIG)


# =========================
# Fonctions utilitaires
# =========================

def normalize_text(value: Any) -> str | None:
    if value is None:
        return None

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value == "":
        return None

    if value.upper() in {"N/A", "NA", "NULL", "NONE", "NAN"}:
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
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    value = value.strip("_")

    return value


def normalize_name(value: Any) -> str | None:
    value = normalize_text(value)

    if value is None:
        return None

    words = value.split(" ")
    cleaned_words = []

    for word in words:
        if not word:
            continue

        apostrophe_parts = word.split("'")
        cleaned_parts = [
            part[:1].upper() + part[1:].lower()
            for part in apostrophe_parts
            if part
        ]

        cleaned_words.append("'".join(cleaned_parts))

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


def normalize_service(value: Any) -> tuple[str, str] | None:
    service_key = normalize_key(value)

    if service_key == "":
        return None

    return SERVICE_MAPPING.get(service_key)


def detect_separator(csv_content: str) -> str:
    first_line = csv_content.splitlines()[0]

    if first_line.count(";") > first_line.count(","):
        return ";"

    return ","


def read_csv_from_minio(client: Minio, object_name: str) -> pd.DataFrame:
    response = client.get_object(MINIO_BUCKET, object_name)

    try:
        content = response.read().decode("utf-8-sig")
        separator = detect_separator(content)

        dataframe = pd.read_csv(
            io.StringIO(content),
            sep=separator,
            dtype=str,
            keep_default_na=False,
        )

        return dataframe

    finally:
        response.close()
        response.release_conn()


def standardize_dataframe(dataframe: pd.DataFrame, source_file: str) -> pd.DataFrame:
    standardized_columns = {}

    for column in dataframe.columns:
        normalized_column = normalize_key(column)
        target_column = COLUMN_MAPPING.get(normalized_column)

        if target_column:
            standardized_columns[column] = target_column

    df = dataframe.rename(columns=standardized_columns)

    expected_columns = [
        "first_name",
        "last_name",
        "age",
        "pathology",
        "phone",
        "comment",
        "service",
    ]

    for column in expected_columns:
        if column not in df.columns:
            df[column] = None

    df = df[expected_columns].copy()

    df["first_name"] = df["first_name"].apply(normalize_name)
    df["last_name"] = df["last_name"].apply(normalize_name)
    df["age"] = df["age"].apply(normalize_age)
    df["pathology"] = df["pathology"].apply(normalize_optional_text)
    df["phone"] = df["phone"].apply(normalize_phone)
    df["comment"] = df["comment"].apply(normalize_optional_text)
    df["source_file"] = source_file

    return df

def is_missing(value: Any) -> bool:
    return value is None or pd.isna(value)

def validate_patient(row: pd.Series) -> list[str]:
    errors = []

    if is_missing(row["first_name"]):
        errors.append("Missing first_name")

    if is_missing(row["last_name"]):
        errors.append("Missing last_name")

    if is_missing(row["age"]):
        errors.append("Invalid age")

    if normalize_service(row["service"]) is None:
        errors.append("Unknown service")

    return errors


# =========================
# Requêtes PostgreSQL
# =========================

def insert_rejected_patient(
    cursor,
    source_file: str,
    raw_data: dict[str, Any],
    rejection_reason: str,
) -> None:
    cleaned_raw_data = clean_for_json(raw_data)

    cursor.execute(
        """
        INSERT INTO rejected_patients (
            source_file,
            raw_data,
            rejection_reason
        )
        VALUES (%s, %s::jsonb, %s);
        """,
        (
            source_file,
            json.dumps(cleaned_raw_data, ensure_ascii=False, allow_nan=False),
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


def patient_already_exists(cursor, patient: pd.Series, service_id: int) -> bool:
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

def clean_for_json(value: Any) -> Any:
    if value is None:
        return None

    if pd.isna(value):
        return None

    if isinstance(value, dict):
        return {
            key: clean_for_json(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            clean_for_json(item)
            for item in value
        ]

    return value

def to_python_int(value: Any) -> int | None:
    if is_missing(value):
        return None

    return int(value)


def to_python_text(value: Any) -> str | None:
    value = normalize_text(value)
    if value is None:
        return None
    return str(value)


def insert_patient(
    cursor,
    patient: pd.Series,
    service_id: int,
) -> None:
    values = (
        to_python_text(patient["first_name"]),
        to_python_text(patient["last_name"]),
        to_python_int(patient["age"]),
        to_python_text(patient["pathology"]),
        to_python_text(patient["phone"]),
        to_python_text(patient["comment"]),
        to_python_int(service_id),
        to_python_text(patient["source_file"]),
    )

    print(f"[insert_patient] values={values}")
    print(f"[insert_patient] types={[type(value).__name__ for value in values]}")

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
        values,
    )

def normalize_phone(value: Any) -> str | None:
    value = normalize_text(value)

    if value is None:
        return None

    # Keep only digits and possible leading +
    value = value.strip()

    # Remove common separators
    value = value.replace(" ", "")
    value = value.replace("-", "")
    value = value.replace(".", "")
    value = value.replace("(", "")
    value = value.replace(")", "")

    # French local format: 0611223344 -> +33611223344
    if re.fullmatch(r"0[1-9]\d{8}", value):
        return "+33" + value[1:]

    # French format without leading 0: 611223344 -> +33611223344
    if re.fullmatch(r"[1-9]\d{8}", value):
        return "+33" + value

    # Already international with +33
    if re.fullmatch(r"\+33[1-9]\d{8}", value):
        return value

    # International French format written as 0033...
    if re.fullmatch(r"0033[1-9]\d{8}", value):
        return "+" + value[2:]

    # If the format is unknown, keep None rather than storing dirty data.
    return None

# =========================
# DAG Airflow
# =========================

@dag(
    dag_id="ingest_hospital_csv",
    description="Read hospital CSV files from MinIO, standardize them and insert into PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hospital", "csv", "minio", "postgres", "pipeline-as-code"],
)
def ingest_hospital_csv():

    @task
    def ingest_csv_files_from_minio() -> dict[str, int]:
        minio_client = get_minio_client()

        objects = list(
            minio_client.list_objects(
                bucket_name=MINIO_BUCKET,
                prefix=MINIO_PREFIX,
                recursive=True,
            )
        )

        csv_objects = [
            obj.object_name
            for obj in objects
            if obj.object_name.endswith(".csv")
        ]

        if not csv_objects:
            raise FileNotFoundError(
                f"No CSV files found in minio://{MINIO_BUCKET}/{MINIO_PREFIX}"
            )

        files_processed = 0
        inserted_patients = 0
        rejected_patients = 0
        duplicated_patients = 0
        inserted_or_updated_services = 0

        with get_postgres_connection() as connection:
            with connection.cursor() as cursor:
                for object_name in sorted(csv_objects):
                    source_file = object_name.split("/")[-1]

                    print(f"[ingestion] Reading minio://{MINIO_BUCKET}/{object_name}")

                    raw_dataframe = read_csv_from_minio(
                        client=minio_client,
                        object_name=object_name,
                    )

                    standardized_dataframe = standardize_dataframe(
                        dataframe=raw_dataframe,
                        source_file=source_file,
                    )

                    files_processed += 1

                    for _, patient in standardized_dataframe.iterrows():
                        errors = validate_patient(patient)

                        if errors:
                            insert_rejected_patient(
                                cursor=cursor,
                                source_file=source_file,
                                raw_data=patient.to_dict(),
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

                        if patient_already_exists(
                            cursor=cursor,
                            patient=patient,
                            service_id=service_id,
                        ):
                            duplicated_patients += 1
                            continue

                        insert_patient(
                            cursor=cursor,
                            patient=patient,
                            service_id=service_id,
                        )

                        inserted_patients += 1

        summary = {
            "files_processed": files_processed,
            "inserted_patients": inserted_patients,
            "rejected_patients": rejected_patients,
            "duplicated_patients": duplicated_patients,
            "inserted_or_updated_services": inserted_or_updated_services,
        }

        print(f"[summary] {summary}")

        return summary

    ingest_csv_files_from_minio()


ingest_hospital_csv()