CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

CREATE USER hospital_user WITH PASSWORD 'hospital_password';
CREATE DATABASE hospital_db OWNER hospital_user;

\connect hospital_db;

CREATE TABLE IF NOT EXISTS services (
    service_id INTEGER PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    floor INTEGER,
    building VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS patients (
    patient_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    birth_date DATE NOT NULL,
    gender VARCHAR(20),
    service_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_patient_service
        FOREIGN KEY (service_id)
        REFERENCES services(service_id)
);