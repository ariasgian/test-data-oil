import sqlite3
import pandas as pd
import os
import extract_data as ed
from pathlib import Path

# --- CONFIGURACIÓN ---
DB_FILE = 'produccion_petrolera.db'

def setup_database():
    """Crea las tablas de la base de datos ejecutando el DDL del schema.sql."""
    SCHEMA_FILE = Path('sql') /'schema.sql'
    print("--- 1. INICIANDO SETUP DE LA BASE DE DATOS ---")
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Base de datos existente '{DB_FILE}' eliminada para un inicio limpio.")
    
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        print(f"Activando claves foráneas...")
        cursor.execute('PRAGMA foreign_keys = ON;')

        print(f"Leyendo '{SCHEMA_FILE}' y creando esquema...")
        with open(SCHEMA_FILE, 'r') as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        
        conn.commit()
        print("✅ Setup de la base de datos completado exitosamente.")
    except sqlite3.Error as e:
        print(f"❌ ERROR en setup_database: {e}")
        raise # Propaga el error para detener el pipeline
    finally:
        if conn:
            conn.close()
def extract_data_from_source():
    """Extrae datos de la URL de la EIA y los devuelve como un DataFrame."""
    print("\n--- 2. INICIANDO EXTRACCIÓN DE DATOS ---")
    try:
        extractor = ed.DataExtractor()
        print("Descargando y extrayendo datos de producción de petróleo...")
        ed.get_gas_production()
        print("Descargando y extrayendo datos de producción de gas...")
        ed.get_oil_production()
        print("Descargando y extrayendo datos de pozos...")        
        df_wells = extractor.download_and_extract_csv(
            'https://www.dec.ny.gov/fs/data/wellDOS.zip',
            Path('data') / 'raw',
            'wellspublic.csv',
            ['API_WellNo', 'Well_Status', 'Operator_number',  'Completion','Surface_Longitude', 'Surface_latitude']
        )
        # Guarda el DataFrame transformado y filtrado
        output = Path('geo') / 'wellspublic.csv'
        df_wells.to_csv(output, index=False)
        print(f"✅ Extracción completada. ")
    except Exception as e:
        print(f"❌ ERROR en extract_data_from_source: {e}")
        raise
def normalize_data():
    transform_columns = ed.DataExtractor().transform_columns
    #convertir columnas de fecha y eliminar outliers
    input_path = Path('data') / 'raw'
    output_path = Path('data') / 'processed'
    file = 'oil_production.csv'
    df = pd.read_csv(input_path / file, low_memory=False)
    df= transform_columns(df)
    df.to_csv(output_path / file, index=False)
    print("Normalizando datos oil...")
    
    file = 'gas_production.csv'
    df = pd.read_csv(input_path / file, low_memory=False)
    df = transform_columns(df)
    df.to_csv(output_path / file, index=False)
    print("Normalizando datos gas...")
    
    # Eliminar outliers de coordenadas
    input_path = Path('geo')
    output_path = Path('geo')
    df_wells = pd.read_csv(input_path / 'wellspublic.csv',low_memory=False)
    df_wells = ed.DataExtractor().drop_outliers(df_wells)
    df_wells.to_csv(output_path / 'wellspublic.csv', index=False)
    print("Outliers de coordenadas eliminados.")
    
def ingest_all_data():
    """Lee los tres archivos CSV procesados y los ingesta en la base de datos SQLite."""
    print("\n--- INGESTANDO TODOS LOS DATOS PROCESADOS ---")
    processed_path = Path('data') / 'processed'
    files = ['oil_production.csv', 'gas_production.csv', 'wellspublic.csv']
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON;')

        # Ingestar oil_production.csv
        oil_df = pd.read_csv(processed_path / files[0])
        if not oil_df.empty:
            oil_df.to_sql('oil_production', conn, if_exists='append', index=False)
            print(f"{len(oil_df)} registros de oil_production cargados.")

        # Ingestar gas_production.csv
        gas_df = pd.read_csv(processed_path / files[1])
        if not gas_df.empty:
            gas_df.to_sql('gas_production', conn, if_exists='append', index=False)
            print(f"{len(gas_df)} registros de gas_production cargados.")
        processed_path = Path('geo')
        # Ingestar wellspublic.csv
        wells_df = pd.read_csv(processed_path / 'wellspublic.csv' )
        if not wells_df.empty:
            wells_df.to_sql('wells_by_county', conn, if_exists='append', index=False)
            print(f"{len(wells_df)} registros de wells_by_county cargados.")

        conn.commit()
        print("✅ Ingesta de todos los datos completada.")
    except Exception as e:
        print(f"❌ ERROR en ingest_all_data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
def geospatial_query():
    """Realiza una consulta geoespacial para obtener la ubicación de los pozos."""
    print("\n--- REALIZANDO CONSULTA GEOESPACIAL ---")
    conn = sqlite3.connect(DB_FILE)
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
    """Convierte el archivo wellspublic.csv en un GeoJSON."""
    import json

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

    with open(output_path, 'w') as f:
        json.dump(geojson, f)
    print(f"GeoJSON generado en {output_path}")
def main():
    """Función principal que orquesta el pipeline ETL."""
    print("====== INICIANDO PIPELINE ETL DE PRODUCCIÓN DE PETRÓLEO ======")
    
    # Part 1-1
    setup_database()
    print("Base de datos configurada correctamente.")
    
    # Part 2: Extraer los datos de la fuente
    print("\n--- INICIANDO ETAPA DE EXTRACCIÓN DE DATOS ---")
    extract_data_from_source()
    normalize_data()
    ingest_all_data()
    print("Datos procesados e ingeridos correctamente.")

    #Part 4: Realizar consulta geoespacial
    print("\n--- INICIANDO CONSULTA GEOESPACIAL ---")
    df=geospatial_query()
    df.to_csv(Path('data') / 'processed'/ 'wells_by_county.csv', index=False)
    wells_to_geojson()
    
    print("Consulta geoespacial completada. Resultados mostrados.")
    
    print("\n====== PIPELINE ETL FINALIZADO EXITOSAMENTE ======")
    
if __name__ == '__main__':
    main()