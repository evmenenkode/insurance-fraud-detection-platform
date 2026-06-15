from pathlib import Path
import ast

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_python_files_are_valid_syntax():
    python_files = [
        path
        for path in PROJECT_ROOT.rglob("*.py")
        if "venv" not in path.parts
    ]

    assert python_files, "No Python files found"

    for file_path in python_files:
        source = file_path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(file_path))


def test_required_project_structure_exists():
    required_paths = [
        "src/ingestion/sql_server/db_setup.sql",
        "src/ingestion/sql_server/enable_cdc.sql",
        "src/ingestion/s3_images/ingest_s3_images_to_bronze.py",
        "src/ingestion/kinesis/ingest_kinesis_data_to_bronze.py",
        "src/transformation/silver/bronze_to_silver.py",
        "src/transformation/gold/silver_to_gold.py",
        "ml/create_damage_predictions_table.py",
        "ml/rule_engine_claim_insights.py",
        "ml/training_dataset.py",
        "requirements.txt",
    ]

    for relative_path in required_paths:
        assert (PROJECT_ROOT / relative_path).exists(), f"Missing {relative_path}"


def test_no_hardcoded_aws_secrets():
    forbidden_patterns = [
        "aws_access_key_id",
        "aws_secret_access_key",
        "AKIA",
        "SECRET_ACCESS_KEY",
    ]

    checked_files = [
    path
    for path in PROJECT_ROOT.rglob("*")
    if path.is_file()
    and "venv" not in path.parts
    and ".git" not in path.parts
    and "tests" not in path.parts
    and path.suffix in {".py", ".sql", ".md", ".yml", ".yaml", ".txt"}
    ]

    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8", errors="ignore")

        for pattern in forbidden_patterns:
            assert pattern not in content, f"Possible secret found in {file_path}"


def test_sql_setup_contains_expected_tables():
    sql_path = PROJECT_ROOT / "src/ingestion/sql_server/db_setup.sql"
    sql = sql_path.read_text(encoding="utf-8").lower()

    expected_terms = [
        "create schema claims_info",
        "create table claims_info.policy",
        "create table claims_info.claims",
        "create table claims_info.customers",
        "primary key",
        "updated_ts",
    ]

    for term in expected_terms:
        assert term in sql, f"Missing SQL setup term: {term}"


def test_cdc_script_contains_expected_configuration():
    sql_path = PROJECT_ROOT / "src/ingestion/sql_server/enable_cdc.sql"
    sql = sql_path.read_text(encoding="utf-8").lower()

    expected_terms = [
        "sp_cdc_enable_db",
        "sp_cdc_enable_table",
        "claims_info",
        "policy",
        "claims",
        "customers",
    ]

    for term in expected_terms:
        assert term in sql, f"Missing CDC term: {term}"