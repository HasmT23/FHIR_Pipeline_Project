import requests
import os
import zipfile
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

def download_data():
    url = "https://github.com/synthetichealth/synthea-sample-data/raw/6796c59993a6b422599020e250454b8d9a83b55c/downloads/synthea_sample_data_fhir_r4_nov2021.zip"
    file_path = "../data/raw/synthea_sample_data_fhir_r4_nov2021.zip"

    # Check if file already exists
    if os.path.exists(file_path):
        print(f"{file_path} already exists. Skipping download.")
        return

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Download the file with error handling
    try:
        print(f"Downloading data from {url}...")
        response = requests.get(url)
        response.raise_for_status()

        # Write the file
        with open(file_path, 'wb') as f:
            f.write(response.content)

        print(f"Successfully downloaded data to {file_path}")

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except ConnectionError as conn_err:
        print(f'Connection error occurred: {conn_err}')
    except Timeout as time_err:
        print(f'Timeout error occurred: {time_err}')
    except RequestException as req_err:
        print(f'Request error occurred: {req_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')



def unzip_data():
    zip_path = "../data/raw/synthea_sample_data_fhir_r4_nov2021.zip"
    extract_to = "../data/raw/"

    # Check if zip file exists
    if not os.path.exists(zip_path):
        print(f"{zip_path} does not exist. Cannot unzip.")
        return

    # Extract with error handling
    try:
        print(f"Extracting {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of files to check if already extracted
            file_list = zip_ref.namelist()

            # Check if files are already extracted
            first_file = os.path.join(extract_to, file_list[0])
            if os.path.exists(first_file):
                print(f"Files already extracted to {extract_to}. Skipping extraction.")
                return

            # Extract all files
            zip_ref.extractall(extract_to)
            print(f"Successfully extracted {len(file_list)} files to {extract_to}")

    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file or is corrupted")
    except PermissionError as perm_err:
        print(f"Permission error: {perm_err}")
    except Exception as err:
        print(f"Error during extraction: {err}")


if __name__ == "__main__":
    download_data()  
    unzip_data()




    

