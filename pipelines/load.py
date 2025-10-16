"""
Module for uploading pandas DataFrames to Google Sheets.

This module provides utility functions for configuring a Google Sheets client using a service 
account and uploading DataFrame contents into a specified worksheet. It leverages the gspread 
library and gspread_dataframe for seamless integration between pandas and Google Sheets.

Requirements:
    - A valid service account JSON key file is needed.
    - The service account's email address must be added to the shared list of the target workbook.
    - The Google Cloud Platform (GCP) project linked with the service account must have both the 
      Google Sheets API and Google Drive API enabled.
    - The service account must be given Editor access to the target Google Sheets workbook.

Functions:
    config_google_sheets(credentials_path: str) -> gspread.client.Client
        Configures and returns an authorized gspread client using a service account.

    upload_to_google_sheets(credentials_path: str, spreadsheet_name: str, df: pd.DataFrame, 
                            sheet_name: str = "Sheet1") -> None
        Uploads a pandas DataFrame to a specified worksheet. If the worksheet doesn't exist, it 
        attempts to create it.

Usage Example:
    from this_module import upload_to_google_sheets

    df = pd.read_csv("my_data.csv")
    credentials_path = "path/to/service_account_credentials.json"
    spreadsheet_name = "My Workbook"
    upload_to_google_sheets(credentials_path, spreadsheet_name, df, sheet_name="Sheet1")
"""


import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def config_google_sheets(credentials_path: str) -> gspread.client.Client:
    """
    Configure and return an authorized gspread client using a service account.

    Parameters:
        credentials_path (str): The path to the JSON keyfile for the Google service account.

    Returns:
        gspread.client.Client: An authorized client for interacting with Google Sheets.

    Raises:
        FileNotFoundError: If the credentials file is not found.
        Exception: For other errors related to credential loading or authorization.
    """
    # Define the required scopes for Google Sheets and Drive
    scope = [
        "https://spreadsheets.google.com/feeds", 
        "https://www.googleapis.com/auth/drive"
    ]

    try:
        # Load credentials from the specified JSON keyfile
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        # Authorize the client with the credentials
        client = gspread.authorize(credentials)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Credentials file not found at: {credentials_path}") from e
    except Exception as e:
        raise Exception("Failed to configure Google Sheets client. Please verify your credentials and scope.") from e

    return client


def upload_to_google_sheets(credentials_path: str,
                            spreadsheet_name: str,
                            df: pd.DataFrame,
                            sheet_name: str = "Sheet1") -> None:
    """
    Upload a pandas DataFrame to a specified worksheet in a Google Sheets spreadsheet.
    
    Parameters:
        credentials_path (str): The path to the JSON keyfile for the Google service account.
        spreadsheet_name (str): The name of the target Google Sheets spreadsheet.
        df (pd.DataFrame): The DataFrame to upload.
        sheet_name (str): The worksheet name where data will be uploaded (default is "Sheet1").
    
    Raises:
        Exception: If the spreadsheet or worksheet cannot be opened or created,
                   or if the data upload fails.
    """
    try:
        # Configure and authorize the Google Sheets client
        client = config_google_sheets(credentials_path)
    except Exception as e:
        raise Exception(f"Error configuring Google Sheets client: {e}") from e

    try:
        # Open the specified spreadsheet by name
        spreadsheet = client.open(spreadsheet_name)
    except Exception as e:
        raise Exception(f"Failed to open spreadsheet '{spreadsheet_name}'. "
                        "Ensure it exists and you have access.") from e

    try:
        # Attempt to open the specified worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
    except Exception as e:
        # If the worksheet is not found, attempt to create it
        try:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=df.shape[0] + 10, cols=df.shape[1])
        except Exception as inner_e:
            raise Exception(f"Failed to open or create worksheet '{sheet_name}' in spreadsheet '{spreadsheet_name}'.") from inner_e

    try:
        # Upload the DataFrame to the worksheet
        set_with_dataframe(worksheet, df)
    except Exception as e:
        raise Exception("Failed to upload DataFrame to Google Sheets.") from e

    print(f"✅ Data successfully uploaded to '{spreadsheet_name}' -> '{sheet_name}'.")


if __name__ == "__main__":

    pass

