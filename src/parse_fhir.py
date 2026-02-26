import json
import pandas as pd
import os
from glob import glob


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_reference_id(reference_dict):
    """
    Extract patient ID from FHIR reference.
    Input: {"reference": "urn:uuid:92fb7efc-5cfd-f8d3-927b-42f8ee099531"}
    Output: "92fb7efc-5cfd-f8d3-927b-42f8ee099531"
    """
    if not reference_dict:
        return None

    reference = reference_dict.get('reference', '')

    if 'urn:uuid:' in reference:
        return reference.split('urn:uuid:')[-1]
    elif '/' in reference:
        return reference.split('/')[-1]

    return reference


def safe_get_coding(codeable_concept, index=0):
    """
    Safely extract code and display from a FHIR CodeableConcept.
    Returns: (code, display) tuple
    """
    if not codeable_concept:
        return None, None

    coding_list = codeable_concept.get('coding', [])
    if not coding_list or len(coding_list) <= index:
        return None, None

    coding = coding_list[index]
    return coding.get('code'), coding.get('display')


def extract_race(patient_resource):
    """
    Extract race from deeply nested Patient extensions.
    Structure: extension ' us-core-race ' extension ' text ' valueString
    """
    extensions = patient_resource.get('extension', [])

    for ext in extensions:
        if 'us-core-race' in ext.get('url', ''):
            inner_extensions = ext.get('extension', [])
            for inner_ext in inner_extensions:
                if inner_ext.get('url') == 'text':
                    return inner_ext.get('valueString')

    return None


def extract_ethnicity(patient_resource):
    """
    Extract ethnicity from deeply nested Patient extensions.
    Structure: extension ' us-core-ethnicity ' extension ' text ' valueString
    """
    extensions = patient_resource.get('extension', [])

    for ext in extensions:
        if 'us-core-ethnicity' in ext.get('url', ''):
            inner_extensions = ext.get('extension', [])
            for inner_ext in inner_extensions:
                if inner_ext.get('url') == 'text':
                    return inner_ext.get('valueString')

    return None


# ============================================================================
# PARSER FUNCTIONS
# ============================================================================

def parse_patients(file_paths):
    """
    Parse Patient resources from FHIR JSON files.
    Returns DataFrame with columns: id, given_name, family_name, gender, birth_date, race, ethnicity, city, state
    """
    records = []

    for i, file_path in enumerate(file_paths, 1):
        if i % 50 == 0:
            print(f"  Parsing patients: {i}/{len(file_paths)} files...")

        try:
            with open(file_path, 'r') as f:
                bundle = json.load(f)

            # Patient is typically the first entry
            for entry in bundle.get('entry', []):
                resource = entry.get('resource', {})

                if resource.get('resourceType') != 'Patient':
                    continue

                # Extract name
                name_list = resource.get('name', [{}])
                name = name_list[0] if name_list else {}
                given_name = name.get('given', [''])[0]
                family_name = name.get('family', '')

                # Extract address
                address_list = resource.get('address', [{}])
                address = address_list[0] if address_list else {}

                records.append({
                    'id': resource.get('id'),
                    'given_name': given_name,
                    'family_name': family_name,
                    'gender': resource.get('gender'),
                    'birth_date': resource.get('birthDate'),
                    'race': extract_race(resource),
                    'ethnicity': extract_ethnicity(resource),
                    'city': address.get('city'),
                    'state': address.get('state')
                })

                break  # Only one patient per file

        except Exception as e:
            print(f"  Error parsing patient from {file_path}: {e}")

    return pd.DataFrame(records)


def parse_conditions(file_paths):
    """
    Parse Condition resources from FHIR JSON files.
    Returns DataFrame with columns: id, patient_id, code, display, clinical_status, onset_date
    """
    records = []

    for i, file_path in enumerate(file_paths, 1):
        if i % 50 == 0:
            print(f"  Parsing conditions: {i}/{len(file_paths)} files...")

        try:
            with open(file_path, 'r') as f:
                bundle = json.load(f)

            for entry in bundle.get('entry', []):
                resource = entry.get('resource', {})

                if resource.get('resourceType') != 'Condition':
                    continue

                # Extract code
                code, display = safe_get_coding(resource.get('code'))

                # Extract clinical status
                clinical_status_code, _ = safe_get_coding(resource.get('clinicalStatus'))

                records.append({
                    'id': resource.get('id'),
                    'patient_id': extract_reference_id(resource.get('subject')),
                    'code': code,
                    'display': display,
                    'clinical_status': clinical_status_code,
                    'onset_date': resource.get('onsetDateTime')
                })

        except Exception as e:
            print(f"  Error parsing conditions from {file_path}: {e}")

    return pd.DataFrame(records)


def parse_observations(file_paths):
    """
    Parse Observation resources from FHIR JSON files.
    Handles three value types: valueQuantity, valueCodeableConcept, and component-based.
    Returns DataFrame with columns: id, patient_id, code, display, value, unit, effective_date, category
    """
    records = []

    for i, file_path in enumerate(file_paths, 1):
        if i % 50 == 0:
            print(f"  Parsing observations: {i}/{len(file_paths)} files...")

        try:
            with open(file_path, 'r') as f:
                bundle = json.load(f)

            for entry in bundle.get('entry', []):
                resource = entry.get('resource', {})

                if resource.get('resourceType') != 'Observation':
                    continue

                # Extract code
                code, display = safe_get_coding(resource.get('code'))

                # Extract category
                category_list = resource.get('category', [])
                category_code = None
                if category_list:
                    category_code, _ = safe_get_coding(category_list[0])

                # Extract value (three possible types)
                value = None
                unit = None

                if 'valueQuantity' in resource:
                    value_qty = resource['valueQuantity']
                    value = value_qty.get('value')
                    unit = value_qty.get('unit')

                elif 'valueCodeableConcept' in resource:
                    _, value = safe_get_coding(resource['valueCodeableConcept'])

                elif 'component' in resource:
                    # For component-based observations (e.g., Blood Pressure)
                    # Create separate records for each component
                    components = resource.get('component', [])
                    for idx, comp in enumerate(components):
                        comp_code, comp_display = safe_get_coding(comp.get('code'))
                        comp_value = None
                        comp_unit = None

                        if 'valueQuantity' in comp:
                            comp_value_qty = comp['valueQuantity']
                            comp_value = comp_value_qty.get('value')
                            comp_unit = comp_value_qty.get('unit')

                        records.append({
                            'id': f"{resource.get('id')}-{idx}",
                            'patient_id': extract_reference_id(resource.get('subject')),
                            'code': comp_code,
                            'display': comp_display,
                            'value': comp_value,
                            'unit': comp_unit,
                            'effective_date': resource.get('effectiveDateTime'),
                            'category': category_code
                        })

                    continue  # Skip the main record append below

                records.append({
                    'id': resource.get('id'),
                    'patient_id': extract_reference_id(resource.get('subject')),
                    'code': code,
                    'display': display,
                    'value': value,
                    'unit': unit,
                    'effective_date': resource.get('effectiveDateTime'),
                    'category': category_code
                })

        except Exception as e:
            print(f"  Error parsing observations from {file_path}: {e}")

    return pd.DataFrame(records)


def parse_encounters(file_paths):
    """
    Parse Encounter resources from FHIR JSON files.
    Returns DataFrame with columns: id, patient_id, encounter_class, type_display, start_date, end_date
    """
    records = []

    for i, file_path in enumerate(file_paths, 1):
        if i % 50 == 0:
            print(f"  Parsing encounters: {i}/{len(file_paths)} files...")

        try:
            with open(file_path, 'r') as f:
                bundle = json.load(f)

            for entry in bundle.get('entry', []):
                resource = entry.get('resource', {})

                if resource.get('resourceType') != 'Encounter':
                    continue

                # Extract class (it's an object, not an array)
                encounter_class = resource.get('class', {}).get('code')

                # Extract type (it's an array)
                type_list = resource.get('type', [])
                type_display = None
                if type_list:
                    _, type_display = safe_get_coding(type_list[0])

                # Extract period
                period = resource.get('period', {})

                records.append({
                    'id': resource.get('id'),
                    'patient_id': extract_reference_id(resource.get('subject')),
                    'encounter_class': encounter_class,
                    'type_display': type_display,
                    'start_date': period.get('start'),
                    'end_date': period.get('end')
                })

        except Exception as e:
            print(f"  Error parsing encounters from {file_path}: {e}")

    return pd.DataFrame(records)


def parse_medication_requests(file_paths):
    """
    Parse MedicationRequest resources from FHIR JSON files.
    Returns DataFrame with columns: id, patient_id, medication_code, medication_display, authored_on, status
    """
    records = []

    for i, file_path in enumerate(file_paths, 1):
        if i % 50 == 0:
            print(f"  Parsing medication requests: {i}/{len(file_paths)} files...")

        try:
            with open(file_path, 'r') as f:
                bundle = json.load(f)

            for entry in bundle.get('entry', []):
                resource = entry.get('resource', {})

                if resource.get('resourceType') != 'MedicationRequest':
                    continue

                # Extract medication code
                med_code, med_display = safe_get_coding(resource.get('medicationCodeableConcept'))

                records.append({
                    'id': resource.get('id'),
                    'patient_id': extract_reference_id(resource.get('subject')),
                    'medication_code': med_code,
                    'medication_display': med_display,
                    'authored_on': resource.get('authoredOn'),
                    'status': resource.get('status')
                })

        except Exception as e:
            print(f"  Error parsing medication requests from {file_path}: {e}")

    return pd.DataFrame(records)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """
    Main function to orchestrate FHIR data parsing.
    Returns dict of DataFrames for each resource type.
    """
    # Get base directory (src folder)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Build path to FHIR data files
    data_pattern = os.path.join(base_dir, '../data/raw/fhir/*.json')
    file_paths = glob(data_pattern)

    print(f"\nFound {len(file_paths)} FHIR JSON files")
    print("=" * 60)

    # Parse each resource type
    print("\n1. Parsing Patients...")
    patients_df = parse_patients(file_paths)
    print(f"    Extracted {len(patients_df)} patients")

    print("\n2. Parsing Conditions...")
    conditions_df = parse_conditions(file_paths)
    print(f"    Extracted {len(conditions_df)} conditions")

    print("\n3. Parsing Observations...")
    observations_df = parse_observations(file_paths)
    print(f"    Extracted {len(observations_df)} observations")

    print("\n4. Parsing Encounters...")
    encounters_df = parse_encounters(file_paths)
    print(f"    Extracted {len(encounters_df)} encounters")

    print("\n5. Parsing Medication Requests...")
    medication_requests_df = parse_medication_requests(file_paths)
    print(f"    Extracted {len(medication_requests_df)} medication requests")

    print("\n" + "=" * 60)
    print("Parsing complete!\n")

    # Return dictionary of DataFrames
    return {
        'patients': patients_df,
        'conditions': conditions_df,
        'observations': observations_df,
        'encounters': encounters_df,
        'medication_requests': medication_requests_df
    }


if __name__ == "__main__":
    # Run the parser
    dataframes = main()

    # Display summary
    print("\nDataFrame Summary:")
    print("-" * 60)
    for name, df in dataframes.items():
        print(f"{name:20s}: {len(df):6d} rows × {len(df.columns):2d} columns")
        print(f"  Columns: {', '.join(df.columns.tolist())}")
        print()
