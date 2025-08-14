"""
Module for extracting, transforming, and saving oil and gas production data.

This module provides the DataExtractor class and utility functions to download data from Excel and ZIP sources, process and clean the data, and save it to CSV files. It is designed for use with US oil and gas production datasets, specifically for West Virginia and Pennsylvania.

Classes:
    DataExtractor: Methods for downloading, extracting, and transforming production data.

Functions:
    get_production: Combines production data from multiple Excel sheets.
    get_oil_production: Downloads and saves oil production data.
    get_gas_production: Downloads and saves gas production data.
"""

import zipfile
from io import BytesIO
from pathlib import Path
import requests
import pandas as pd

class DataExtractor:
    """
    Class for extracting and transforming oil and gas production data from various sources.
    """
    def get_dataframe(self, url: str, sheet_name: str, skiprows: int) -> pd.DataFrame:
        """
        Download an Excel sheet and return a DataFrame with selected columns and renamed headers.
        Args:
            url (str): URL of the Excel file.
            sheet_name (str): Name of the sheet to read.
            skiprows (int): Number of rows to skip at the top.
        Returns:
            pd.DataFrame: DataFrame with columns year_month, production, and county.
        """
        df = pd.read_excel(url, sheet_name=sheet_name, skiprows=skiprows, engine='openpyxl')
        # Replace line breaks in column names with spaces        
        df = df.iloc[:, [0, 2]]
        df.columns = ['year_month', 'production']
        # Add column with the state name
        df['county'] = sheet_name
        return df
    def download_and_extract_csv(self, zip_url: str, extract_to: Path, csv_filename: str, columns: list = None) -> pd.DataFrame:
        """
        Download a ZIP file, extract a CSV, filter and rename columns.
        Args:
            zip_url (str): URL of the ZIP file.
            extract_to (Path): Path to extract the CSV file.
            csv_filename (str): Name of the CSV file inside the ZIP.
            columns (list, optional): List of columns to keep.
        Returns:
            pd.DataFrame: DataFrame with selected and renamed columns.
        """
        response = requests.get(zip_url, timeout=30)
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extract(csv_filename, path=extract_to)
            csv_path = extract_to / csv_filename
            df = pd.read_csv(csv_path,low_memory=False)
            # Filter columns if specified (first)
            if columns is not None:
                df = df[columns]
            # Rename columns
            rename_dict = {
                'API_WellNo': 'id',
                'Well_Status': 'county',
                'Operator_number': 'operator',
                'Completion': 'status',
                'Surface_Longitude': 'longitude',
                'Surface_latitude': 'latitude'
            }
            df = df.rename(columns=rename_dict)
        return df
    def transform_columns(self, df) -> pd.DataFrame:
        """
        Convert the 'year_month' column to datetime format.
        Args:
            df (pd.DataFrame): Input DataFrame.
        Returns:
            pd.DataFrame: DataFrame with transformed 'year_month' column.
        """
        df['year_month'] = pd.to_datetime(df['year_month'], errors='coerce')
        return df
    def drop_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows with zero or missing longitude/latitude values.
        Args:
            df (pd.DataFrame): Input DataFrame.
        Returns:
            pd.DataFrame: DataFrame without outlier coordinates.
        """
        df = df[(df['longitude'] != 0) & (df['latitude'] != 0)]
        df = df.dropna(subset=['longitude', 'latitude'])
        return df
extractor = DataExtractor()
def get_production(url: str, sheet_configs: list) -> pd.DataFrame:
    """
    Combine production data from multiple Excel sheets into a single DataFrame.
    Args:
        url (str): URL of the Excel file.
        sheet_configs (list): List of tuples (sheet_name, skiprows).
    Returns:
        pd.DataFrame: Combined DataFrame for all sheets.
    """
    df_combined = None
    for state, skiprows in sheet_configs:
        df = extractor.get_dataframe(url, state, skiprows)
        if df_combined is not None:
            df_combined = pd.concat([df_combined, df], ignore_index=True)
        else:
            df_combined = df
    return df_combined
def get_oil_production():
    """
    Download and save oil production data for specified states.
    Returns:
        pd.DataFrame: Oil production DataFrame.
    """
    url = 'https://www.eia.gov/petroleum/production/xls/comp-stat-oil.xlsx'
    sheets = [('WV', 2), ('PA', 3)]
    df_oil = get_production(url, sheets)
    output=Path('data') / 'raw' / 'oil_production.csv'
    df_oil.to_csv(output, index=False)
    return df_oil 
def get_gas_production():
    """
    Download and save gas production data for specified states.
    Returns:
        pd.DataFrame: Gas production DataFrame.
    """
    url = 'https://www.eia.gov/petroleum/production/xls/comp-stat-gas.xlsx'
    sheets = [('WV', 2), ('PA', 3)]
    df_gas = get_production(url, sheets)
    output=Path('data') / 'raw' / 'gas_production.csv'
    df_gas.to_csv(output, index=False)
    return df_gas

