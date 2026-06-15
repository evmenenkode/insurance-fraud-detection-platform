USE claims_dev;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'claims_info'
)
BEGIN
    EXEC('CREATE SCHEMA claims_info');
END;
GO

DROP TABLE IF EXISTS claims_info.claims;
DROP TABLE IF EXISTS claims_info.policy;
DROP TABLE IF EXISTS claims_info.customers;
GO

CREATE TABLE claims_info.customers (
    customer_id INT NOT NULL,
    date_of_birth VARCHAR(50) NULL,
    borough VARCHAR(50) NULL,
    neighborhood VARCHAR(50) NULL,
    zip_code VARCHAR(20) NULL,
    customer_name VARCHAR(100) NULL,
    updated_ts DATETIME2 NOT NULL CONSTRAINT DF_customers_updated_ts DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_customers PRIMARY KEY (customer_id)
);
GO

CREATE TABLE claims_info.policy (
    policy_no VARCHAR(50) NOT NULL,
    cust_id INT NOT NULL,
    policy_type VARCHAR(50) NULL,
    pol_issue_date DATE NULL,
    pol_eff_date DATE NULL,
    pol_expiry_date DATE NULL,
    make VARCHAR(50) NULL,
    model VARCHAR(50) NULL,
    model_year INT NULL,
    chassis_no VARCHAR(50) NULL,
    use_of_vehicle VARCHAR(100) NULL,
    product VARCHAR(100) NULL,
    sum_insured DECIMAL(18,2) NULL,
    premium DECIMAL(18,2) NULL,
    deductible DECIMAL(18,2) NULL,
    updated_ts DATETIME2 NOT NULL CONSTRAINT DF_policy_updated_ts DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_policy PRIMARY KEY (policy_no),
    CONSTRAINT FK_policy_customers FOREIGN KEY (cust_id)
        REFERENCES claims_info.customers(customer_id)
);
GO

CREATE TABLE claims_info.claims (
    claim_no VARCHAR(50) NOT NULL,
    policy_no VARCHAR(50) NOT NULL,
    claim_date DATE NULL,
    months_as_customer INT NULL,
    injury BIGINT NULL,
    property BIGINT NULL,
    vehicle BIGINT NULL,
    total BIGINT NULL,
    collision_type VARCHAR(50) NULL,
    number_of_vehicles_involved INT NULL,
    driver_age FLOAT NULL,
    insured_relationship VARCHAR(50) NULL,
    licence_issue_date DATE NULL,
    incident_date DATE NULL,
    incident_hour INT NULL,
    claim_type VARCHAR(50) NULL,
    severity VARCHAR(50) NULL,
    number_of_witnesses INT NULL,
    suspicious_activity BIT NULL,
    updated_ts DATETIME2 NOT NULL CONSTRAINT DF_claims_updated_ts DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_claims PRIMARY KEY (claim_no),
    CONSTRAINT FK_claims_policy FOREIGN KEY (policy_no)
        REFERENCES claims_info.policy(policy_no)
);
GO