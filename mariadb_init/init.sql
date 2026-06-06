CREATE DATABASE IF NOT EXISTS mlops;

USE mlops;

CREATE TABLE IF NOT EXISTS loan_data (
    id INT,
    annual_income DOUBLE,
    debt_to_income_ratio DOUBLE,
    credit_score INT,
    loan_amount DOUBLE,
    interest_rate DOUBLE,
    gender VARCHAR(50),
    marital_status VARCHAR(50),
    education_level VARCHAR(50),
    employment_status VARCHAR(50),
    loan_purpose VARCHAR(100),
    grade_subgrade VARCHAR(50),
    loan_paid_back FLOAT
) ENGINE=ColumnStore;
