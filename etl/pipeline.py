"""
ETL pipeline for oil and gas production data.

This module provides functions to set up a SQLite database, extract and normalize oil, gas, 
and well data from external sources, ingest the processed data into the database, 
and perform geospatial queries and exports. The pipeline is designed for automated data processing 
and analysis of oil and gas production in the United States.

Functions:
    setup_database: Initializes the database schema.
    extract_data_from_source: Downloads and extracts raw data files.
    normalize_data: Cleans and transforms the extracted data.
    ingest_all_data: Loads processed data into the database.
    geospatial_query: Performs geospatial analysis on well data.
    wells_to_geojson: Converts well data to GeoJSON format.
    main: Orchestrates the ETL pipeline steps.
"""

import sqlite3
import json
import os
from pathlib import Path
import pandas as pd
import extract_data as ed


# --- CONFIGURATION ---
db_file = 'produccion_petrolera.db'

def setup_database():
    """
    Set up the SQLite database by creating tables from the schema.sql file.
    If the database already exists, it will be deleted for a clean start.
    Enables foreign key constraints and commits the schema creation.
    Raises:
        sqlite3.Error: If any error occurs during setup.
    """
    schema_file = Path('sql') / 'schema.sql'
    print("--- 1. STARTING DATABASE SETUP ---")
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Existing database '{db_file}' removed for a clean start.")
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("Enabling foreign keys...")
        cursor.execute('PRAGMA foreign_keys = ON;')
        print(f"Reading '{schema_file}' and creating schema...")
        with open(schema_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        conn.commit()
        print("✅ Database setup completed successfully.")
    except sqlite3.Error as e:
        print(f"❌ ERROR in setup_database: {e}")
        raise # Propagate the error to stop the pipeline
    finally:
        if conn:
            conn.close()
def extract_data_from_source():
    """
    Extracts oil, gas, and wells data from external sources and saves them as CSV files.
    Downloads and processes oil and gas production data, and well location data.
    Raises:
        Exception: If any error occurs during extraction.
    """
    print("\n--- 2. STARTING DATA EXTRACTION ---")
    try:
        extractor = ed.DataExtractor()
        print("Downloading and extracting oil production data...")
        ed.get_gas_production()
        print("Downloading and extracting gas production data...")
        ed.get_oil_production()
        print("Downloading and extracting wells data...")
        df_wells = extractor.download_and_extract_csv(
            'https://www.dec.ny.gov/fs/data/wellDOS.zip',
            Path('data') / 'raw',
            'wellspublic.csv',
            ['API_WellNo', 'Well_Status', 'Operator_number',  'Completion','Surface_Longitude', 'Surface_latitude']
        )
        # Save the transformed and filtered DataFrame
        output = Path('geo') / 'wellspublic.csv'
        df_wells.to_csv(output, index=False)
        print("✅ Extraction completed. ")
    except Exception as e:
        print(f"❌ ERROR in extract_data_from_source: {e}")
        raise
def normalize_data():
    """
    Normalizes oil and gas production data by converting date columns and removing outliers.
    Also removes coordinate outliers from wells data and saves the cleaned files.
    """
    transform_columns = ed.DataExtractor().transform_columns
    # Convert date columns and remove outliers
    input_path = Path('data') / 'raw'
    output_path = Path('data') / 'processed'
    file = 'oil_production.csv'
    df = pd.read_csv(input_path / file, low_memory=False)
    df= transform_columns(df)
    df.to_csv(output_path / file, index=False)
    print("Normalizing oil data...")
    file = 'gas_production.csv'
    df = pd.read_csv(input_path / file, low_memory=False)
    df = transform_columns(df)
    df.to_csv(output_path / file, index=False)
    print("Normalizing gas data...")
    # Remove coordinate outliers
    input_path = Path('geo')
    output_path = Path('geo')
    df_wells = pd.read_csv(input_path / 'wellspublic.csv',low_memory=False)
    df_wells = ed.DataExtractor().drop_outliers(df_wells)
    df_wells.to_csv(output_path / 'wellspublic.csv', index=False)
    print("Coordinate outliers removed.")
def ingest_all_data():
    """
    Loads processed CSV files into the SQLite database tables.
    Ingests oil, gas, and wells data, and commits the transaction.
    Rolls back if any error occurs during ingestion.
    """
    print("\n--- INGESTING ALL PROCESSED DATA ---")
    processed_path = Path('data') / 'processed'
    files = ['oil_production.csv', 'gas_production.csv', 'wellspublic.csv']
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON;')

        # Ingest oil_production.csv
        oil_df = pd.read_csv(processed_path / files[0])
        if not oil_df.empty:
            oil_df.to_sql('oil_production', conn, if_exists='append', index=False)
            print(f"{len(oil_df)} oil_production records loaded.")

        # Ingest gas_production.csv
        gas_df = pd.read_csv(processed_path / files[1])
        if not gas_df.empty:
            gas_df.to_sql('gas_production', conn, if_exists='append', index=False)
            print(f"{len(gas_df)} gas_production records loaded.")
        processed_path = Path('geo')
        # Ingest wellspublic.csv
        wells_df = pd.read_csv(processed_path / 'wellspublic.csv' )
        if not wells_df.empty:
            wells_df.to_sql('wells_by_county', conn, if_exists='append', index=False)
            print(f"{len(wells_df)} wells_by_county records loaded.")

        conn.commit()
        print("✅ All data ingestion completed.")
    except Exception as e:
        print(f"❌ ERROR in ingest_all_data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
def geospatial_query():
    """
    Executes a geospatial query to count wells by county from the database.
    Returns:
        pd.DataFrame: DataFrame with county and well count.
    """
    print("\n--- PERFORMING GEOSPATIAL QUERY ---")
    conn = sqlite3.connect(db_file)
    query = """
        SELECT 
            county, 
            COUNT(*) AS well_count
        FROM wells_by_county
        group by county    
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
def wells_to_geojson():
    """
    Converts the wells CSV file to a GeoJSON file for geospatial visualization.
    Reads well locations and properties, and writes them as GeoJSON features.
    """
    input_path = Path('geo')  / 'wellspublic.csv'
    output_path = Path('data') / 'processed' / 'wellspublic.geojson'
    df = pd.read_csv(input_path)

    features = []
    for _, row in df.iterrows():        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['longitude'], row['latitude']]
            },
            "properties": {k: row[k] for k in df.columns if k not in ['longitud', 'latitude']}
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f)
    print(f"GeoJSON generated at {output_path}")
def main():
    """
    Main function to orchestrate the ETL pipeline steps:
    - Set up the database
    - Extract and normalize data
    - Ingest data into the database
    - Perform geospatial queries and export results
    """
    print("====== STARTING OIL PRODUCTION ETL PIPELINE ======")
    # Part 1-1
    setup_database()
    print("Database configured successfully.")
    # Part 2: Extract data from the source
    print("\n--- STARTING DATA EXTRACTION STAGE ---")
    extract_data_from_source()
    normalize_data()
    ingest_all_data()
    print("Data processed and ingested successfully.")

    # Part 4: Perform geospatial query
    print("\n--- STARTING GEOSPATIAL QUERY ---")
    df=geospatial_query()
    df.to_csv(Path('data') / 'processed'/ 'wells_by_county.csv', index=False)
    wells_to_geojson()    
    print("Geospatial query completed. Results shown.")    
    print("\n====== ETL PIPELINE COMPLETED SUCCESSFULLY ======")    
if __name__ == '__main__':
    main()
