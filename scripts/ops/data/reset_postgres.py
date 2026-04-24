import os
import sys
import argparse

import logging
from pathlib import Path

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("Error: 'psycopg' module not found. Please install dependencies from requirements.txt")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Error: 'python-dotenv' module not found. Please install dependencies from requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_project_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")

def load_environment():
    root = get_project_root()
    env_path = root / ".env"
    if env_path.exists():
        logger.info(f"Loading environment from {env_path}")
        load_dotenv(env_path)
    else:
        logger.warning(f".env file not found at {env_path}")

def get_dsn(args_dsn: str = None) -> str:
    if args_dsn:
        return args_dsn
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        logger.error("POSTGRES_DSN not found in environment or arguments.")
        sys.exit(1)
    return dsn

def confirm_reset(force: bool):
    if force:
        return
    
    print("WARNING: This operation will DESTROY all data in the target database.")
    response = input("Are you sure you want to continue? (y/N): ").strip().lower()
    if response != 'y':
        logger.info("Operation aborted by user.")
        sys.exit(0)

def reset_database(conn):
    logger.info("Resetting database objects (destructive)...")
    with conn.cursor() as cur:
        # Find all schemas except system ones
        cur.execute("""
            SELECT n.nspname AS schema_name
            FROM pg_namespace n
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'public')
              AND n.nspname NOT LIKE 'pg_toast%%'
              AND n.nspname NOT LIKE 'pg_temp_%%'
        """)
        schemas = [row['schema_name'] for row in cur.fetchall()]
        
        for schema in schemas:
            logger.info(f"Dropping schema: {schema}")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        
        logger.info("Recreating public schema...")
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO CURRENT_USER")
        cur.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")

def apply_migrations(conn, migrations_dir: Path):
    logger.info(f"Applying migrations from: {migrations_dir}")
    
    if not migrations_dir.exists():
        logger.error(f"Migrations directory not found: {migrations_dir}")
        sys.exit(1)
        
    with conn.cursor() as cur:
        # Get list of .sql files sorted by name
        sql_files = sorted(migrations_dir.glob("*.sql"))
        
        for sql_file in sql_files:
            logger.info(f"Applying: {sql_file.name}")
            try:
                sql_content = sql_file.read_text(encoding='utf-8')
                cur.execute(sql_content)
            except Exception as e:
                logger.error(f"Failed to apply migration {sql_file.name}: {e}")
                raise e

def main():
    parser = argparse.ArgumentParser(description="Reset Postgres database and apply migrations.")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt.")
    parser.add_argument("--dsn", help="Postgres DSN (overrides env var).")
    parser.add_argument("--migrations-dir", help="Path to migrations directory.")
    
    args = parser.parse_args()
    
    load_environment()
    confirm_reset(args.force)
    
    dsn = get_dsn(args.dsn)
    
    root = get_project_root()
    default_migrations_dir = root / "deploy" / "sql" / "postgres" / "migrations"
    migrations_dir = Path(args.migrations_dir) if args.migrations_dir else default_migrations_dir
    
    try:
        # Connect to database
        # autocommit=True is often needed for DROP SCHEMA / CREATE SCHEMA if they can't run in transaction block
        # usually they can, but let's stick to standard connection context manager which handles transactions
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            reset_database(conn)
            apply_migrations(conn, migrations_dir)
            
            # Connection context manager commits on exit if no exception
            logger.info("Database reset and migrations applied successfully.")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
