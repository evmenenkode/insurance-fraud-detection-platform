/*
========================================================
ENABLE CDC (Change Data Capture) FOR SQL SERVER SOURCE
========================================================

PURPOSE:
This script enables Change Data Capture (CDC) on the SQL Server database
and selected source tables.

WHY WE NEED THIS:
- CDC allows us to capture INSERT / UPDATE / DELETE operations
- It provides incremental data extraction capability
- It is the foundation for building a lakehouse ingestion pipeline
  (SQL Server → Databricks Bronze → Silver → Gold)

HOW IT WILL BE USED:
1. Databricks ingestion jobs will read CDC change tables
2. Only changed records will be loaded into Bronze layer
3. Downstream transformations (dbt) will build clean Silver models

IMPORTANT NOTES:
- This should be executed ONLY once per database
- Re-running may cause "already exists" errors
- Requires SQL Server Agent (for CDC cleanup jobs)
========================================================
*/

USE claims_dev;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = DB_NAME()
      AND is_cdc_enabled = 1
)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = 'claims_info_policy'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'claims_info',
        @source_name = N'policy',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = 'claims_info_claims'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'claims_info',
        @source_name = N'claims',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = 'claims_info_customers'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'claims_info',
        @source_name = N'customers',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO

EXEC sys.sp_cdc_help_change_data_capture;
GO