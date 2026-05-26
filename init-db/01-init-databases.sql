CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

CREATE USER hospital_user WITH PASSWORD 'hospital_password';
CREATE DATABASE hospital_db OWNER hospital_user;

\connect hospital_db;

CREATE TABLE IF NOT EXISTS services (
    service_id SERIAL PRIMARY KEY,
    service_code VARCHAR(50) UNIQUE NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS patients (
    patient_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    age INTEGER NOT NULL,
    pathology TEXT,
    phone VARCHAR(50),
    comment TEXT,
    service_id INTEGER NOT NULL,
    source_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_patients_services
        FOREIGN KEY (service_id)
        REFERENCES services(service_id),

    CONSTRAINT chk_patient_age
        CHECK (age >= 0 AND age <= 120)
);

CREATE TABLE IF NOT EXISTS rejected_patients (
    rejected_id SERIAL PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    raw_data JSONB NOT NULL,
    rejection_reason TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE services OWNER TO hospital_user;
ALTER TABLE patients OWNER TO hospital_user;
ALTER TABLE rejected_patients OWNER TO hospital_user;

GRANT ALL PRIVILEGES ON DATABASE hospital_db TO hospital_user;
GRANT USAGE, CREATE ON SCHEMA public TO hospital_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hospital_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hospital_user;