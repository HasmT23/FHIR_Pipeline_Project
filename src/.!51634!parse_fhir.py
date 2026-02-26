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
