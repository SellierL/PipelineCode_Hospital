import os
import time
import psycopg2


def wait_for_database():
    while True:
        try:
            connection = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
            )
            return connection
        except psycopg2.OperationalError:
            print("[python-app] Waiting for PostgreSQL...")
            time.sleep(3)


def main():
    connection = wait_for_database()

    with connection.cursor() as cursor:
        cursor.execute("SELECT patient_id, first_name, last_name, age FROM patients;")
        patients = cursor.fetchall()

        print("[python-app] Patients found:")
        for patient in patients:
            print(patient)

    connection.close()


if __name__ == "__main__":
    main()