"""
FHIR Analytics Engine
Provides SQL query functions for population health, clinical utilization,
medication, lab analytics, and risk prediction queries.
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_path():
    """
    Get the path to the FHIR SQLite database.
    Works from src/ directory.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, '..', 'data', 'fhir_data.db')


def execute_query(sql, params=None):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        sql (str): SQL query string
        params (tuple, optional): Parameters for parameterized queries

    Returns:
        pd.DataFrame: Query results
    """
    db_path = get_db_path()

    with sqlite3.connect(db_path) as conn:
        if params:
            df = pd.read_sql_query(sql, conn, params=params)
        else:
            df = pd.read_sql_query(sql, conn)

    return df


# ============================================================================
# POPULATION HEALTH QUERIES
# ============================================================================

def get_age_gender_distribution():
    """
    Get patient distribution by age group and gender.
    Returns DataFrame with columns: age_group, gender, count
    """
    sql = """
        SELECT
            CASE
                WHEN age BETWEEN 0 AND 10 THEN '0-10'
                WHEN age BETWEEN 11 AND 20 THEN '11-20'
                WHEN age BETWEEN 21 AND 30 THEN '21-30'
                WHEN age BETWEEN 31 AND 40 THEN '31-40'
                WHEN age BETWEEN 41 AND 50 THEN '41-50'
                WHEN age BETWEEN 51 AND 60 THEN '51-60'
                WHEN age BETWEEN 61 AND 70 THEN '61-70'
                WHEN age BETWEEN 71 AND 80 THEN '71-80'
                ELSE '81+'
            END as age_group,
            gender,
            COUNT(*) as count
        FROM (
            SELECT
                id,
                gender,
                CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) as age
            FROM patients
        )
        GROUP BY age_group, gender
        ORDER BY age_group, gender
    """
    return execute_query(sql)


def get_top_conditions(n=10):
    """
    Get the top N most prevalent conditions by unique patient count.

    Args:
        n (int): Number of top conditions to return

    Returns:
        DataFrame with columns: display, patient_count
    """
    sql = """
        SELECT
            display,
            COUNT(DISTINCT patient_id) as patient_count
        FROM conditions
        WHERE display IS NOT NULL
        GROUP BY display
        ORDER BY patient_count DESC
        LIMIT ?
    """
    return execute_query(sql, params=(n,))


def get_condition_prevalence_by_age():
    """
    Get cumulative condition prevalence by age group for animation.
    Returns data for age ranges: 0-10, 0-20, 0-30, 0-40, 0-50, 0-60, 0-70, 0-80+
    This allows for "stacking" animation showing disease burden accumulation.

    Returns:
        DataFrame with columns: age_upper_bound, condition_display, patient_count
    """
    sql = """
        WITH patient_ages AS (
            SELECT
                id,
                CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) as age
            FROM patients
        ),
        top_conditions AS (
            SELECT display
            FROM conditions
            GROUP BY display
            ORDER BY COUNT(DISTINCT patient_id) DESC
            LIMIT 10
        ),
        age_bounds AS (
            SELECT 10 as upper_bound UNION ALL
            SELECT 20 UNION ALL
            SELECT 30 UNION ALL
            SELECT 40 UNION ALL
            SELECT 50 UNION ALL
            SELECT 60 UNION ALL
            SELECT 70 UNION ALL
            SELECT 80 UNION ALL
            SELECT 100
        )
        SELECT
            ab.upper_bound,
            '0-' || ab.upper_bound as age_range,
            c.display as condition_display,
            COUNT(DISTINCT c.patient_id) as patient_count
        FROM age_bounds ab
        CROSS JOIN top_conditions tc
        LEFT JOIN conditions c ON c.display = tc.display
        LEFT JOIN patient_ages pa ON pa.id = c.patient_id AND pa.age <= ab.upper_bound
        WHERE c.display IS NOT NULL
        GROUP BY ab.upper_bound, c.display
        ORDER BY ab.upper_bound, patient_count DESC
    """
    return execute_query(sql)


def get_race_distribution():
    """
    Get patient distribution by race.

    Returns:
        DataFrame with columns: race, count
    """
    sql = """
        SELECT
            race,
            COUNT(*) as count
        FROM patients
        WHERE race IS NOT NULL
        GROUP BY race
        ORDER BY count DESC
    """
    return execute_query(sql)


def get_geographic_distribution():
    """
    Get patient distribution by state.

    Returns:
        DataFrame with columns: state, count
    """
    sql = """
        SELECT
            state,
            COUNT(*) as count
        FROM patients
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY count DESC
    """
    return execute_query(sql)


# ============================================================================
# CLINICAL UTILIZATION QUERIES
# ============================================================================

def get_encounter_type_breakdown():
    """
    Get encounter counts by encounter class (ambulatory, inpatient, emergency).

    Returns:
        DataFrame with columns: encounter_class, count
    """
    sql = """
        SELECT
            CASE encounter_class
                WHEN 'AMB' THEN 'Ambulatory'
                WHEN 'EMER' THEN 'Emergency'
                WHEN 'IMP' THEN 'Inpatient'
                ELSE encounter_class
            END as encounter_class,
            COUNT(*) as count
        FROM encounters
        WHERE encounter_class IS NOT NULL
        GROUP BY encounter_class
        ORDER BY count DESC
    """
    return execute_query(sql)


def get_encounters_per_patient():
    """
    Get distribution of encounter counts per patient.

    Returns:
        DataFrame with columns: patient_id, given_name, family_name, encounter_count
    """
    sql = """
        SELECT
            p.id as patient_id,
            p.given_name,
            p.family_name,
            COUNT(e.id) as encounter_count
        FROM patients p
        LEFT JOIN encounters e ON p.id = e.patient_id
        GROUP BY p.id, p.given_name, p.family_name
        ORDER BY encounter_count DESC
    """
    return execute_query(sql)


def get_high_utilizers(threshold=50):
    """
    Get patients with encounter counts above threshold, with their top conditions.

    Args:
        threshold (int): Minimum number of encounters to be considered high utilizer

    Returns:
        DataFrame with columns: patient_id, given_name, family_name, encounter_count, top_conditions
    """
    sql = """
        WITH high_util_patients AS (
            SELECT
                p.id as patient_id,
                p.given_name,
                p.family_name,
                COUNT(e.id) as encounter_count
            FROM patients p
            JOIN encounters e ON p.id = e.patient_id
            GROUP BY p.id, p.given_name, p.family_name
            HAVING encounter_count >= ?
        ),
        patient_conditions AS (
            SELECT
                c.patient_id,
                GROUP_CONCAT(c.display, '; ') as top_conditions
            FROM conditions c
            WHERE c.patient_id IN (SELECT patient_id FROM high_util_patients)
                AND c.clinical_status = 'active'
            GROUP BY c.patient_id
        )
        SELECT
            hu.patient_id,
            hu.given_name,
            hu.family_name,
            hu.encounter_count,
            COALESCE(pc.top_conditions, 'No active conditions') as top_conditions
        FROM high_util_patients hu
        LEFT JOIN patient_conditions pc ON hu.patient_id = pc.patient_id
        ORDER BY hu.encounter_count DESC
    """
    return execute_query(sql, params=(threshold,))


def get_encounters_by_year():
    """
    Get encounter data by year and month for bubble chart animation.

    Returns:
        DataFrame with columns: year, month, encounter_class, count, avg_patient_age
    """
    sql = """
        SELECT
            strftime('%Y', e.start_date) as year,
            CAST(strftime('%m', e.start_date) AS INTEGER) as month,
            CASE e.encounter_class
                WHEN 'AMB' THEN 'Ambulatory'
                WHEN 'EMER' THEN 'Emergency'
                WHEN 'IMP' THEN 'Inpatient'
                ELSE e.encounter_class
            END as encounter_class,
            COUNT(*) as count,
            AVG(CAST((julianday(e.start_date) - julianday(p.birth_date)) / 365.25 AS INTEGER)) as avg_patient_age
        FROM encounters e
        JOIN patients p ON e.patient_id = p.id
        WHERE e.start_date IS NOT NULL
        GROUP BY year, month, encounter_class
        ORDER BY year, month, encounter_class
    """
    return execute_query(sql)


def get_conditions_driving_encounters():
    """
    Find which conditions are associated with the most encounters.
    JOINs conditions to encounters where patient_id matches and dates overlap.

    Returns:
        DataFrame with columns: condition_display, encounter_count, patient_count
    """
    sql = """
        SELECT
            c.display as condition_display,
            COUNT(DISTINCT e.id) as encounter_count,
            COUNT(DISTINCT c.patient_id) as patient_count
        FROM conditions c
        JOIN encounters e ON c.patient_id = e.patient_id
            AND date(e.start_date) >= date(c.onset_date)
        WHERE c.display IS NOT NULL
        GROUP BY c.display
        ORDER BY encounter_count DESC
        LIMIT 20
    """
    return execute_query(sql)


# ============================================================================
# MEDICATION QUERIES
# ============================================================================

def get_top_medications(n=10):
    """
    Get the top N most frequently prescribed medications.

    Args:
        n (int): Number of top medications to return

    Returns:
        DataFrame with columns: medication_display, prescription_count
    """
    sql = """
        SELECT
            medication_display,
            COUNT(*) as prescription_count
        FROM medication_requests
        WHERE medication_display IS NOT NULL
        GROUP BY medication_display
        ORDER BY prescription_count DESC
        LIMIT ?
    """
    return execute_query(sql, params=(n,))


def get_polypharmacy_distribution():
    """
    Get distribution of unique medication counts per patient.
    Flags patients with 5+ medications as polypharmacy.

    Returns:
        DataFrame with columns: patient_id, given_name, family_name, medication_count, polypharmacy_flag
    """
    sql = """
        SELECT
            p.id as patient_id,
            p.given_name,
            p.family_name,
            COUNT(DISTINCT m.medication_display) as medication_count,
            CASE
                WHEN COUNT(DISTINCT m.medication_display) >= 5 THEN 1
                ELSE 0
            END as polypharmacy_flag
        FROM patients p
        LEFT JOIN medication_requests m ON p.id = m.patient_id
        GROUP BY p.id, p.given_name, p.family_name
        ORDER BY medication_count DESC
    """
    return execute_query(sql)


def get_medications_by_condition(condition_display):
    """
    Get medications prescribed for patients with a specific condition.

    Args:
        condition_display (str): Condition name to filter by

    Returns:
        DataFrame with columns: medication_display, patient_count
    """
    sql = """
        SELECT
            m.medication_display,
            COUNT(DISTINCT m.patient_id) as patient_count
        FROM medication_requests m
        WHERE m.patient_id IN (
            SELECT DISTINCT patient_id
            FROM conditions
            WHERE display LIKE ?
        )
        AND m.medication_display IS NOT NULL
        GROUP BY m.medication_display
        ORDER BY patient_count DESC
    """
    return execute_query(sql, params=(f'%{condition_display}%',))


def get_medication_trends_by_age():
    """
    Get average medication counts by age group.

    Returns:
        DataFrame with columns: age_group, avg_medication_count, patient_count
    """
    sql = """
        WITH patient_ages AS (
            SELECT
                id,
                CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) as age,
                CASE
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 0 AND 10 THEN '0-10'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 11 AND 20 THEN '11-20'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 21 AND 30 THEN '21-30'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 31 AND 40 THEN '31-40'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 41 AND 50 THEN '41-50'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 51 AND 60 THEN '51-60'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 61 AND 70 THEN '61-70'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 71 AND 80 THEN '71-80'
                    ELSE '81+'
                END as age_group
            FROM patients
        ),
        patient_med_counts AS (
            SELECT
                m.patient_id,
                COUNT(DISTINCT m.medication_display) as med_count
            FROM medication_requests m
            GROUP BY m.patient_id
        )
        SELECT
            pa.age_group,
            AVG(COALESCE(pmc.med_count, 0)) as avg_medication_count,
            COUNT(pa.id) as patient_count
        FROM patient_ages pa
        LEFT JOIN patient_med_counts pmc ON pa.id = pmc.patient_id
        GROUP BY pa.age_group
        ORDER BY pa.age_group
    """
    return execute_query(sql)


def get_patient_medication_timeline():
    """
    Get medication accumulation over time for top 10 high-utilizer patients.
    Returns year-by-year medication counts to animate polypharmacy development.

    Returns:
        DataFrame with columns: patient_name, year, medication_count
    """
    sql = """
        WITH high_utilizers AS (
            SELECT
                p.id as patient_id,
                p.given_name || ' ' || p.family_name as patient_name
            FROM patients p
            JOIN medication_requests m ON p.id = m.patient_id
            GROUP BY p.id, patient_name
            ORDER BY COUNT(DISTINCT m.medication_display) DESC
            LIMIT 10
        )
        SELECT
            hu.patient_name,
            strftime('%Y', m.authored_on) as year,
            COUNT(DISTINCT m.medication_display) as medication_count
        FROM high_utilizers hu
        JOIN medication_requests m ON hu.patient_id = m.patient_id
        WHERE m.authored_on IS NOT NULL
        GROUP BY hu.patient_name, year
        ORDER BY hu.patient_name, year
    """
    return execute_query(sql)


# ============================================================================
# LAB ANALYTICS QUERIES
# ============================================================================

def get_lab_value_distribution(observation_code):
    """
    Get distribution of lab values for a specific observation type.

    Args:
        observation_code (str): LOINC code for the observation

    Returns:
        DataFrame with columns: value, unit, effective_date
    """
    sql = """
        SELECT
            CAST(value AS REAL) as value,
            unit,
            date(effective_date) as effective_date
        FROM observations
        WHERE code = ?
            AND value IS NOT NULL
            AND CAST(value AS REAL) IS NOT NULL
        ORDER BY effective_date
    """
    return execute_query(sql, params=(observation_code,))


def get_abnormal_lab_values():
    """
    Get observations outside normal reference ranges.

    Reference ranges:
    - Glucose (2339-0, 2345-7): 70-100 mg/dL
    - Total Cholesterol (2093-3): <200 mg/dL
    - BMI (39156-5): 18.5-24.9 kg/m2
    - Systolic BP (8480-6): <120 mmHg
    - Diastolic BP (8462-4): <80 mmHg
    - Hemoglobin A1c (4548-4): <5.7 %

    Returns:
        DataFrame with columns: patient_id, code, display, value, unit, effective_date, abnormal_flag
    """
    sql = """
        SELECT
            patient_id,
            code,
            display,
            CAST(value AS REAL) as value,
            unit,
            date(effective_date) as effective_date,
            CASE
                -- Glucose
                WHEN code IN ('2339-0', '2345-7') AND (CAST(value AS REAL) < 70 OR CAST(value AS REAL) > 100) THEN 'Abnormal'
                -- Total Cholesterol
                WHEN code = '2093-3' AND CAST(value AS REAL) >= 200 THEN 'Abnormal'
                -- BMI
                WHEN code = '39156-5' AND (CAST(value AS REAL) < 18.5 OR CAST(value AS REAL) > 24.9) THEN 'Abnormal'
                -- Systolic BP
                WHEN code = '8480-6' AND CAST(value AS REAL) >= 120 THEN 'Abnormal'
                -- Diastolic BP
                WHEN code = '8462-4' AND CAST(value AS REAL) >= 80 THEN 'Abnormal'
                -- Hemoglobin A1c
                WHEN code = '4548-4' AND CAST(value AS REAL) >= 5.7 THEN 'Abnormal'
                ELSE 'Normal'
            END as abnormal_flag
        FROM observations
        WHERE code IN ('2339-0', '2345-7', '2093-3', '39156-5', '8480-6', '8462-4', '4548-4')
            AND value IS NOT NULL
            AND CAST(value AS REAL) IS NOT NULL
        ORDER BY patient_id, effective_date
    """
    return execute_query(sql)


def get_lab_trends_by_age():
    """
    Get average lab values by age group for key lab types.

    Returns:
        DataFrame with columns: age_group, lab_type, avg_value, patient_count
    """
    sql = """
        WITH patient_ages AS (
            SELECT
                id,
                CASE
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 0 AND 10 THEN '0-10'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 11 AND 20 THEN '11-20'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 21 AND 30 THEN '21-30'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 31 AND 40 THEN '31-40'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 41 AND 50 THEN '41-50'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 51 AND 60 THEN '51-60'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 61 AND 70 THEN '61-70'
                    WHEN CAST((julianday('now') - julianday(birth_date)) / 365.25 AS INTEGER) BETWEEN 71 AND 80 THEN '71-80'
                    ELSE '81+'
                END as age_group
            FROM patients
        )
        SELECT
            pa.age_group,
            CASE
                WHEN o.code IN ('2339-0', '2345-7') THEN 'Glucose'
                WHEN o.code = '2093-3' THEN 'Total Cholesterol'
                WHEN o.code = '39156-5' THEN 'BMI'
                WHEN o.code = '8480-6' THEN 'Systolic BP'
                WHEN o.code = '8462-4' THEN 'Diastolic BP'
                WHEN o.code = '4548-4' THEN 'Hemoglobin A1c'
            END as lab_type,
            AVG(CAST(o.value AS REAL)) as avg_value,
            COUNT(DISTINCT o.patient_id) as patient_count
        FROM patient_ages pa
        JOIN observations o ON pa.id = o.patient_id
        WHERE o.code IN ('2339-0', '2345-7', '2093-3', '39156-5', '8480-6', '8462-4', '4548-4')
            AND o.value IS NOT NULL
            AND CAST(o.value AS REAL) IS NOT NULL
        GROUP BY pa.age_group, lab_type
        ORDER BY pa.age_group, lab_type
    """
    return execute_query(sql)


def get_patient_lab_timeline(patient_id):
    """
    Get all lab observations for a specific patient ordered chronologically.

    Args:
        patient_id (str): Patient ID

    Returns:
        DataFrame with columns: code, display, value, unit, effective_date
    """
    sql = """
        SELECT
            code,
            display,
            CAST(value AS REAL) as value,
            unit,
            date(effective_date) as effective_date
        FROM observations
        WHERE patient_id = ?
            AND value IS NOT NULL
            AND CAST(value AS REAL) IS NOT NULL
            AND category IN ('vital-signs', 'laboratory')
        ORDER BY effective_date ASC
    """
    return execute_query(sql, params=(patient_id,))


def get_lab_trajectories_by_year():
    """
    Get all patients' lab values over time with normal/abnormal flags for animation.

    Returns:
        DataFrame with columns: year, patient_id, lab_type, value, abnormal_flag
    """
    sql = """
        SELECT
            strftime('%Y', effective_date) as year,
            patient_id,
            CASE
                WHEN code IN ('2339-0', '2345-7') THEN 'Glucose'
                WHEN code = '2093-3' THEN 'Total Cholesterol'
                WHEN code = '39156-5' THEN 'BMI'
                WHEN code = '8480-6' THEN 'Systolic BP'
                WHEN code = '8462-4' THEN 'Diastolic BP'
                WHEN code = '4548-4' THEN 'Hemoglobin A1c'
            END as lab_type,
            CAST(value AS REAL) as value,
            CASE
                -- Glucose
                WHEN code IN ('2339-0', '2345-7') AND (CAST(value AS REAL) < 70 OR CAST(value AS REAL) > 100) THEN 'Abnormal'
                -- Total Cholesterol
                WHEN code = '2093-3' AND CAST(value AS REAL) >= 200 THEN 'Abnormal'
                -- BMI
                WHEN code = '39156-5' AND (CAST(value AS REAL) < 18.5 OR CAST(value AS REAL) > 24.9) THEN 'Abnormal'
                -- Systolic BP
                WHEN code = '8480-6' AND CAST(value AS REAL) >= 120 THEN 'Abnormal'
                -- Diastolic BP
                WHEN code = '8462-4' AND CAST(value AS REAL) >= 80 THEN 'Abnormal'
                -- Hemoglobin A1c
                WHEN code = '4548-4' AND CAST(value AS REAL) >= 5.7 THEN 'Abnormal'
                ELSE 'Normal'
            END as abnormal_flag
        FROM observations
        WHERE code IN ('2339-0', '2345-7', '2093-3', '39156-5', '8480-6', '8462-4', '4548-4')
            AND value IS NOT NULL
            AND CAST(value AS REAL) IS NOT NULL
            AND effective_date IS NOT NULL
        ORDER BY year, patient_id, lab_type
    """
    return execute_query(sql)


# ============================================================================
# RISK & READMISSION QUERIES
# ============================================================================

def get_readmission_candidates():
    """
    Find 30-day readmissions: encounters where the same patient had another
    encounter within 30 days. Returns readmission rate overall and by encounter type.

    Returns:
        DataFrame with columns: encounter_class, total_encounters, readmissions, readmission_rate
    """
    sql = """
        WITH readmissions AS (
            SELECT DISTINCT
                e1.id as initial_encounter_id,
                e1.patient_id,
                e1.encounter_class,
                e1.start_date as initial_date,
                e2.start_date as readmission_date
            FROM encounters e1
            JOIN encounters e2 ON e1.patient_id = e2.patient_id
                AND e2.id != e1.id
                AND date(e2.start_date) BETWEEN date(e1.start_date) AND date(e1.start_date, '+30 days')
                AND date(e2.start_date) > date(e1.start_date)
        )
        SELECT
            CASE e.encounter_class
                WHEN 'AMB' THEN 'Ambulatory'
                WHEN 'EMER' THEN 'Emergency'
                WHEN 'IMP' THEN 'Inpatient'
                ELSE e.encounter_class
            END as encounter_class,
            COUNT(DISTINCT e.id) as total_encounters,
            COUNT(DISTINCT r.initial_encounter_id) as readmissions,
            ROUND(CAST(COUNT(DISTINCT r.initial_encounter_id) AS REAL) / COUNT(DISTINCT e.id) * 100, 2) as readmission_rate
        FROM encounters e
        LEFT JOIN readmissions r ON e.id = r.initial_encounter_id
        GROUP BY e.encounter_class
        ORDER BY readmission_rate DESC
    """
    return execute_query(sql)


def get_patient_complexity_scores():
    """
    Calculate patient complexity scores based on:
    - Count of unique conditions
    - Count of unique medications
    - Total encounters
    - Count of unique abnormal lab values

    Returns:
        DataFrame with columns: patient_id, patient_name, condition_count, medication_count,
                                encounter_count, abnormal_lab_count, complexity_score
    """
    sql = """
        WITH patient_conditions AS (
            SELECT
                patient_id,
                COUNT(DISTINCT display) as condition_count
            FROM conditions
            WHERE clinical_status = 'active'
            GROUP BY patient_id
        ),
        patient_medications AS (
            SELECT
                patient_id,
                COUNT(DISTINCT medication_display) as medication_count
            FROM medication_requests
            GROUP BY patient_id
        ),
        patient_encounters AS (
            SELECT
                patient_id,
                COUNT(*) as encounter_count
            FROM encounters
            GROUP BY patient_id
        ),
        patient_abnormal_labs AS (
            SELECT
                patient_id,
                COUNT(DISTINCT code) as abnormal_lab_count
            FROM observations
            WHERE code IN ('2339-0', '2345-7', '2093-3', '39156-5', '8480-6', '8462-4', '4548-4')
                AND value IS NOT NULL
                AND CAST(value AS REAL) IS NOT NULL
                AND (
                    (code IN ('2339-0', '2345-7') AND (CAST(value AS REAL) < 70 OR CAST(value AS REAL) > 100))
                    OR (code = '2093-3' AND CAST(value AS REAL) >= 200)
                    OR (code = '39156-5' AND (CAST(value AS REAL) < 18.5 OR CAST(value AS REAL) > 24.9))
                    OR (code = '8480-6' AND CAST(value AS REAL) >= 120)
                    OR (code = '8462-4' AND CAST(value AS REAL) >= 80)
                    OR (code = '4548-4' AND CAST(value AS REAL) >= 5.7)
                )
            GROUP BY patient_id
        )
        SELECT
            p.id as patient_id,
            p.given_name || ' ' || p.family_name as patient_name,
            COALESCE(pc.condition_count, 0) as condition_count,
            COALESCE(pm.medication_count, 0) as medication_count,
            COALESCE(pe.encounter_count, 0) as encounter_count,
            COALESCE(pal.abnormal_lab_count, 0) as abnormal_lab_count,
            (COALESCE(pc.condition_count, 0) * 2 +
             COALESCE(pm.medication_count, 0) * 1.5 +
             COALESCE(pe.encounter_count, 0) * 0.5 +
             COALESCE(pal.abnormal_lab_count, 0) * 3) as complexity_score
        FROM patients p
        LEFT JOIN patient_conditions pc ON p.id = pc.patient_id
        LEFT JOIN patient_medications pm ON p.id = pm.patient_id
        LEFT JOIN patient_encounters pe ON p.id = pe.patient_id
        LEFT JOIN patient_abnormal_labs pal ON p.id = pal.patient_id
        ORDER BY complexity_score DESC
    """
    return execute_query(sql)


def get_complexity_by_condition_count():
    """
    Get patient complexity scores grouped by number of chronic conditions.
    Used for animated histogram showing how complexity shifts with condition count.

    Returns:
        DataFrame with columns: condition_count_group, complexity_score, patient_count
    """
    sql = """
        WITH patient_complexity AS (
            SELECT
                p.id as patient_id,
                COUNT(DISTINCT c.display) as condition_count,
                (COUNT(DISTINCT c.display) * 2 +
                 COUNT(DISTINCT m.medication_display) * 1.5 +
                 COUNT(DISTINCT e.id) * 0.5) as complexity_score
            FROM patients p
            LEFT JOIN conditions c ON p.id = c.patient_id AND c.clinical_status = 'active'
            LEFT JOIN medication_requests m ON p.id = m.patient_id
            LEFT JOIN encounters e ON p.id = e.patient_id
            GROUP BY p.id
        )
        SELECT
            CASE
                WHEN condition_count = 0 THEN '0 Conditions'
                WHEN condition_count = 1 THEN '1 Condition'
                WHEN condition_count = 2 THEN '2 Conditions'
                ELSE '3+ Conditions'
            END as condition_count_group,
            complexity_score,
            1 as patient_count
        FROM patient_complexity
        ORDER BY condition_count, complexity_score
    """
    return execute_query(sql)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Testing FHIR Analytics Engine")
    print("=" * 60)

    # Test 1: Age-Gender Distribution
    print("\n1. Age-Gender Distribution:")
    df = get_age_gender_distribution()
    print(df.head(10))

    # Test 2: Top Conditions
    print("\n2. Top 10 Conditions:")
    df = get_top_conditions(10)
    print(df.head(10))

    # Test 3: Readmission Rates
    print("\n3. 30-Day Readmission Rates:")
    df = get_readmission_candidates()
    print(df)

    # Test 4: Patient Complexity Scores
    print("\n4. Top 5 Highest Complexity Patients:")
    df = get_patient_complexity_scores()
    print(df.head(5))

    # Test 5: Polypharmacy
    print("\n5. Polypharmacy Distribution Summary:")
    df = get_polypharmacy_distribution()
    print(f"Total patients: {len(df)}")
    print(f"Patients with polypharmacy (5+ meds): {df['polypharmacy_flag'].sum()}")
    print(f"Polypharmacy rate: {df['polypharmacy_flag'].sum() / len(df) * 100:.1f}%")

    print("\n" + "=" * 60)
    print("All tests completed successfully!")
