import pandas as pd
from pathlib import Path
import requests
import zipfile
from io import BytesIO

class DataExtractor:
    def get_dataframe(self, url: str, sheet_name: str, skiprows: int) -> pd.DataFrame:
        df = pd.read_excel(url, sheet_name=sheet_name, skiprows=skiprows, engine='openpyxl')
        # Reemplaza saltos de línea en los nombres de las columnas por espacios
        df.columns = [col.replace('\n', ' ') for col in df.columns]
        # Agrega columna con el nombre del estado
        df['State'] = sheet_name
        return df
    def download_and_extract_csv(self, zip_url: str, extract_to: Path, csv_filename: str) -> pd.DataFrame:
        response = requests.get(zip_url)
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extract(csv_filename, path=extract_to)
            csv_path = extract_to / csv_filename
            df = pd.read_csv(csv_path,low_memory=False)
        return df
extractor = DataExtractor()

def get_production(url: str, sheet_configs: list) -> pd.DataFrame:
    """
    sheet_configs: lista de tuplas (sheet_name, skiprows)
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
    url = 'https://www.eia.gov/petroleum/production/xls/comp-stat-oil.xlsx'
    sheets = [('WV', 2), ('PA', 3)]
    df_oil = get_production(url, sheets)
    output=Path('data') / 'raw' / 'oil_production.csv'
    df_oil.to_csv(output, index=False)
    return df_oil 
def get_gas_production():
    url = 'https://www.eia.gov/petroleum/production/xls/comp-stat-gas.xlsx'
    sheets = [('WV', 2), ('PA', 3)]
    df_gas = get_production(url, sheets)
    output=Path('data') / 'raw' / 'gas_production.csv'
    df_gas.to_csv(output, index=False)
    return df_gas
extractor.download_and_extract_csv('https://www.dec.ny.gov/fs/data/wellDOS.zip', Path('data') / 'raw', 'wellspublic.csv')
