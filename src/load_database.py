import sqlite3
import pandas as pd
import os
from parse_fhir import main as parse_fhir_data


# ============================================================================
# DATABASE SCHEMA DEFINITIONS
# ============================================================================

def create_tables(conn):
    """
    Create all database tables with proper schemas and foreign key constraints.
    Order matters: create parent tables (patients) before child tables.
    """
    cursor = conn.cursor()

    # Enable foreign key support (CRITICAL - disabled by default in SQLite)
    cursor.execute("PRAGMA foreign_keys = ON")

    print("\nCreating database schema...")

    # 1. PATIENTS TABLE (parent table - no dependencies)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            id TEXT PRIMARY KEY,
            given_name TEXT,
            family_name TEXT,
            gender TEXT,
            birth_date TEXT,
            race TEXT,
            ethnicity TEXT,
            city TEXT,
            state TEXT
        )
    """)
    print("  ✓ Created patients table")

    # 2. CONDITIONS TABLE (child table - references patients)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conditions (
            id TEXT PRIMARY KEY,
            patient_id TEXT NOT NULL,
            code TEXT,
            display TEXT,
            clinical_status TEXT,
            onset_date TEXT,
            FOREIGN KEY (patient_id) REFERENCES patients(id)
                ON DELETE CASCADE
        )
    """)
    print("  ✓ Created conditions table")

    # 3. OBSERVATIONS TABLE (child table - references patients)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id TEXT PRIMARY KEY,
            patient_id TEXT NOT NULL,
            code TEXT,
            display TEXT,
            value TEXT,
            unit TEXT,
            effective_date TEXT,
            category TEXT,
            FOREIGN KEY (patient_id) REFERENCES patients(id)
                ON DELETE CASCADE
        )
    """)
    print("  ✓ Created observations table")

    # 4. ENCOUNTERS TABLE (child table - references patients)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS encounters (
            id TEXT PRIMARY KEY,
            patient_id TEXT NOT NULL,
            encounter_class TEXT,
            type_display TEXT,
            start_date TEXT,
            end_date TEXT,
            FOREIGN KEY (patient_id) REFERENCES patients(id)
                ON DELETE CASCADE
        )
    """)
    print("  ✓ Created encounters table")

    # 5. MEDICATION_REQUESTS TABLE (child table - references patients)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS medication_requests (
            id TEXT PRIMARY KEY,
            patient_id TEXT NOT NULL,
            medication_code TEXT,
            medication_display TEXT,
            authored_on TEXT,
            status TEXT,
            FOREIGN KEY (patient_id) REFERENCES patients(id)
                ON DELETE CASCADE
        )
    """)
    print("  ✓ Created medication_requests table")

    conn.commit()


def create_indexes(conn):
    """
    Create indexes on foreign keys and commonly queried columns.
    Indexes dramatically improve JOIN and WHERE query performance.
    """
    cursor = conn.cursor()

    print("\nCreating indexes...")

    # Index foreign keys for JOIN performance
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_conditions_patient_id
        ON conditions(patient_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_observations_patient_id
        ON observations(patient_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_encounters_patient_id
        ON encounters(patient_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_medication_requests_patient_id
        ON medication_requests(patient_id)
    """)

    # Index commonly queried columns
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_observations_code
        ON observations(code)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_observations_category
        ON observations(category)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_conditions_code
        ON conditions(code)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_encounters_class
        ON encounters(encounter_class)
    """)

    print("  ✓ Created indexes on foreign keys")
    print("  ✓ Created indexes on commonly queried columns")

    conn.commit()


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_data(conn, dataframes):
    """
    Load DataFrames into database tables.
    Uses pandas to_sql() with if_exists='append' to preserve foreign key constraints.
    """
    print("\nLoading data into database...")

    try:
        # Load patients first (parent table)
        patients_df = dataframes['patients']
        patients_df.to_sql('patients', conn, if_exists='append', index=False)
        print(f"  ✓ Loaded {len(patients_df)} patients")

        # Load child tables (order doesn't matter among these)
        conditions_df = dataframes['conditions']
        conditions_df.to_sql('conditions', conn, if_exists='append', index=False)
        print(f"  ✓ Loaded {len(conditions_df)} conditions")

        observations_df = dataframes['observations']
        observations_df.to_sql('observations', conn, if_exists='append', index=False)
        print(f"  ✓ Loaded {len(observations_df)} observations")

        encounters_df = dataframes['encounters']
        encounters_df.to_sql('encounters', conn, if_exists='append', index=False)
        print(f"  ✓ Loaded {len(encounters_df)} encounters")

        medication_requests_df = dataframes['medication_requests']
        medication_requests_df.to_sql('medication_requests', conn, if_exists='append', index=False)
        print(f"  ✓ Loaded {len(medication_requests_df)} medication requests")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"\n  ✗ Error loading data: {e}")
        raise


# ============================================================================
# VERIFICATION FUNCTIONS
# ============================================================================

def verify_load(conn, dataframes):
    """
    Verify that data was loaded correctly by checking row counts
    and testing foreign key relationships.
    """
    cursor = conn.cursor()

    print("\nVerifying database...")

    # Check row counts match
    tables = ['patients', 'conditions', 'observations', 'encounters', 'medication_requests']
    all_match = True

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        db_count = cursor.fetchone()[0]
        df_count = len(dataframes[table])

        if db_count == df_count:
            print(f"  ✓ {table}: {db_count} rows")
        else:
            print(f"  ✗ {table}: Expected {df_count} rows, got {db_count}")
            all_match = False

    if not all_match:
        raise Exception("Row count mismatch detected!")

    # Test foreign key relationships with a sample query
    print("\nTesting foreign key relationships...")

    cursor.execute("""
        SELECT
            p.given_name,
            p.family_name,
            COUNT(DISTINCT c.id) as condition_count,
            COUNT(DISTINCT o.id) as observation_count,
            COUNT(DISTINCT e.id) as encounter_count,
            COUNT(DISTINCT m.id) as medication_count
        FROM patients p
        LEFT JOIN conditions c ON p.id = c.patient_id
        LEFT JOIN observations o ON p.id = o.patient_id
        LEFT JOIN encounters e ON p.id = e.patient_id
        LEFT JOIN medication_requests m ON p.id = m.patient_id
        GROUP BY p.id
        LIMIT 5
    """)

    results = cursor.fetchall()

    if results:
        print("  ✓ Foreign key relationships working")
        print("\n  Sample patient data:")
        print("  " + "-" * 80)
        print(f"  {'Name':<25} {'Conditions':<12} {'Observations':<14} {'Encounters':<12} {'Medications'}")
        print("  " + "-" * 80)
        for row in results:
            name = f"{row[0]} {row[1]}"
            print(f"  {name:<25} {row[2]:<12} {row[3]:<14} {row[4]:<12} {row[5]}")
    else:
        print("  ✗ No data returned from JOIN query")
        raise Exception("Foreign key relationship test failed!")


def get_database_info(db_path):
    """
    Get database file size and other metadata.
    """
    if os.path.exists(db_path):
        size_bytes = os.path.getsize(db_path)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    return 0


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """
    Main function to orchestrate database creation and data loading.
    """
    print("\nCreating FHIR SQLite Database")
    print("=" * 60)

    # Get base directory (src folder)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Define database path
    db_path = os.path.join(base_dir, '../data/fhir_data.db')

    # Remove existing database if it exists
    if os.path.exists(db_path):
        print(f"\nRemoving existing database at {db_path}")
        os.remove(db_path)

    # Step 1: Parse FHIR data to get DataFrames
    print("\nStep 1: Parsing FHIR JSON files...")
    print("-" * 60)
    dataframes = parse_fhir_data()

    # Step 2: Create database connection
    print("\nStep 2: Creating database connection...")
    print("-" * 60)
    conn = sqlite3.connect(db_path)
    print(f"  ✓ Connected to database at {db_path}")

    try:
        # Step 3: Create tables with foreign keys
        print("\nStep 3: Creating database schema...")
        print("-" * 60)
        create_tables(conn)

        # Step 4: Load data
        print("\nStep 4: Loading data...")
        print("-" * 60)
        load_data(conn, dataframes)

        # Step 5: Create indexes
        print("\nStep 5: Creating indexes...")
        print("-" * 60)
        create_indexes(conn)

        # Step 6: Verify data load
        print("\nStep 6: Verifying database...")
        print("-" * 60)
        verify_load(conn, dataframes)

        # Success!
        print("\n" + "=" * 60)
        print("Database created successfully!")
        print("=" * 60)
        print(f"\nDatabase location: {db_path}")
        print(f"Database size: {get_database_info(db_path):.1f} MB")

        # Print summary statistics
        print("\nDatabase Summary:")
        print("-" * 60)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM patients")
        print(f"  Patients:            {cursor.fetchone()[0]:>8,}")

        cursor.execute("SELECT COUNT(*) FROM conditions")
        print(f"  Conditions:          {cursor.fetchone()[0]:>8,}")

        cursor.execute("SELECT COUNT(*) FROM observations")
        print(f"  Observations:        {cursor.fetchone()[0]:>8,}")

        cursor.execute("SELECT COUNT(*) FROM encounters")
        print(f"  Encounters:          {cursor.fetchone()[0]:>8,}")

        cursor.execute("SELECT COUNT(*) FROM medication_requests")
        print(f"  Medication Requests: {cursor.fetchone()[0]:>8,}")

        print("\nYou can now query this database with SQL!")
        print("Example: sqlite3 ../data/fhir_data.db")
        print()

    except Exception as e:
        print(f"\n✗ Error creating database: {e}")
        raise

    finally:
        # Always close the connection
        conn.close()


if __name__ == "__main__":
    main()
