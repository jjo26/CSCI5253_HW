CREATE TABLE animal_dim (
    animal_id  VARCHAR(32) PRIMARY KEY,
    name VARCHAR(32),
    dob DATE,
    animal_type VARCHAR(10),
    breed VARCHAR(64),
    color VARCHAR(32),
    sex VARCHAR(32)
);

CREATE TABLE processed_dim (
    processed_id INT PRIMARY KEY,
    processed_ts TIMESTAMP,
    year INT,
    month INT,
    day INT
);

CREATE TABLE outcome_type_dim (
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR(32)
);

CREATE TABLE outcome_subtype_dim(
    outcome_subtype_id INT PRIMARY KEY,
    outcome_subtype VARCHAR(32)
);

CREATE TABLE repro_dim(
    reproductive_status_id INT PRIMARY KEY,
    reproductive_status VARCHAR(32)
);

CREATE TABLE visit_fact(
    visit_id SERIAL PRIMARY KEY,
    animal_id VARCHAR(32) REFERENCES animal_dim(animal_id),
    processed_id INT REFERENCES processed_dim(processed_id),
    outcome_type_id INT REFERENCES outcome_type_dim(outcome_type_id),
    outcome_subtype_id INT REFERENCES outcome_subtype_dim(outcome_subtype_id),
    reproductive_status_id INT REFERENCES repro_dim(reproductive_status_id)
);

