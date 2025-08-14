import sqlite3
import pandas as pd
import os
import extract_data as ed
from pathlib import Path

# --- CONFIGURACIÓN ---
DB_FILE = 'produccion_petrolera.db'
SCHEMA_FILE = Path('sql') /'schema.sql'
EIA_URL = 'https://www.eia.gov/petroleum/production/xls/crude-oil-natural-gas.xlsx'
STATES_TO_FILTER = ['West Virginia', 'Pennsylvania']


def setup_database():
    """Crea las tablas de la base de datos ejecutando el DDL del schema.sql."""
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
        output = Path('geo') / 'wells_by_county.csv'
        df_wells.to_csv(output, index=False)
        print(f"✅ Extracción completada. ")
    except Exception as e:
        print(f"❌ ERROR en extract_data_from_source: {e}")
        raise


def transform_and_load_data(df):
    """Transforma el DataFrame y carga los datos en la base de datos SQLite."""
    print("\n--- 3. INICIANDO TRANSFORMACIÓN Y CARGA DE DATOS ---")
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('PRAGMA foreign_keys = ON;')

        # --- TRANSFORMACIÓN Y CARGA DE DIMENSIONES ---
        # Como los datos de origen no tienen operadores ni pozos, crearemos registros "dummy".
        # Esta es una práctica común cuando los datos de origen y destino no coinciden 1 a 1.
        
        # 1. Cargar Operador por defecto
        cursor.execute("INSERT OR IGNORE INTO operators (operator_name) VALUES (?)", ('Default Operator',))
        operator_id = cursor.execute("SELECT operator_id FROM operators WHERE operator_name = ?", ('Default Operator',)).fetchone()[0]
        print(f"Operador por defecto cargado con ID: {operator_id}")

        # 2. Cargar Estados, Pozos "dummy" y mapearlos
        state_well_map = {}
        state_codes = {'West Virginia': 'WV', 'Pennsylvania': 'PA'}
        
        for state_name in df['State'].unique():
            # Cargar estado
            cursor.execute("INSERT OR IGNORE INTO states (state_name, state_code) VALUES (?, ?)", (state_name, state_codes.get(state_name, 'N/A')))
            state_id = cursor.execute("SELECT state_id FROM states WHERE state_name = ?", (state_name,)).fetchone()[0]
            
            # Crear un pozo "agregado" para ese estado, ya que no tenemos datos a nivel de pozo
            well_api = f"{state_codes.get(state_name)}-AGGREGATED-01"
            cursor.execute("""
                INSERT INTO wells (api_well_number, status, latitude, longitude, state_id, operator_id)
                VALUES (?, 'Active', 0.0, 0.0, ?, ?)
            """, (well_api, state_id, operator_id))
            well_id = cursor.lastrowid
            state_well_map[state_name] = well_id
            print(f"Estado '{state_name}' (ID: {state_id}) y Pozo Ficticio (ID: {well_id}) cargados.")

        # --- CARGA DE DATOS DE PRODUCCIÓN (TABLA DE HECHOS) ---
        print("Cargando registros de producción...")
        records_to_load = []
        for index, row in df.iterrows():
            well_id = state_well_map[row['State']]
            # Transformar la fecha al formato YYYY-MM-01
            prod_date = row['Month'].strftime('%Y-%m-01')
            volume = row['Crude Oil Production (Thousand Barrels)']
            
            records_to_load.append((well_id, prod_date, 'Oil', volume, 'Thousand Barrels'))
            
        cursor.executemany("""
            INSERT INTO production_volumes (well_id, production_date, product_type, volume, unit)
            VALUES (?, ?, ?, ?, ?)
        """, records_to_load)
        
        conn.commit()
        print(f"✅ Carga de datos completada. {len(records_to_load)} registros de producción insertados.")
        
    except sqlite3.Error as e:
        print(f"❌ ERROR en transform_and_load_data: {e}")
        if conn:
            conn.rollback() # Revertir cambios si hay un error
        raise
    finally:
        if conn:
            conn.close()
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
    df_wells = pd.read_csv(input_path / 'wells_by_county.csv',low_memory=False)
    df_wells = ed.DataExtractor().drop_outliers(df_wells)
    df_wells.to_csv(output_path / 'wells_by_county.csv', index=False)
    print("Outliers de coordenadas eliminados.")
    
    

def main():
    """Función principal que orquesta el pipeline ETL."""
    print("====== INICIANDO PIPELINE ETL DE PRODUCCIÓN DE PETRÓLEO ======")
    
    # Paso 1: Crear la estructura de la base de datos
    setup_database()
    
    # Paso 2: Extraer los datos de la fuente
    extract_data_from_source()
    
    # Paso 3: Transformar y cargar los datos en la base de datos
    # if production_df is not None and not production_df.empty:
    normalize_data()
    
    # print("\n====== PIPELINE ETL FINALIZADO EXITOSAMENTE ======")


if __name__ == '__main__':
    main()