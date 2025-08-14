# Oil & Gas Production ETL Pipeline

This project provides an automated ETL (Extract, Transform, Load) pipeline for oil and gas production data in the United States. It downloads, processes, and analyzes production and well location data, storing results in a SQLite database and exporting geospatial outputs for further analysis.

## Features

- **Database Setup:** Initializes a SQLite database using a schema defined in [`sql/schema.sql`](sql/schema.sql).
- **Data Extraction:** Downloads raw oil, gas, and well location data from external sources.
- **Data Normalization:** Cleans and transforms extracted data, including date conversion and outlier removal.
- **Data Ingestion:** Loads processed data into database tables for analysis.
- **Geospatial Analysis:** Performs queries to count wells by county and exports results as CSV and GeoJSON files.

## Project Structure

```
etl/
    pipeline.py           # Main ETL pipeline script
    extract_data.py       # Data extraction utilities
geo/
data/
    raw/                  # Raw downloaded data
    processed/            # Cleaned and processed data
sql/
    schema.sql            # Database schema
    analytics_queries.sql # Example analytics queries
produccion_petrolera.db   # SQLite database file
requirements.txt          # Python dependencies
```

## Usage

1. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

2. **Run the ETL pipeline:**
   ```sh
   python etl/pipeline.py
   ```

   The pipeline will:
   - Set up the database
   - Download and process data
   - Ingest data into the database
   - Perform geospatial queries and export results

3. **Outputs:**
   - Processed CSV files in [`data/processed/`](data/processed/)
   - Well location GeoJSON in [`data/processed/wellspublic.geojson`](data/processed/wellspublic.geojson)
   - SQLite database: [`produccion_petrolera.db`](produccion_petrolera.db)

## Requirements

- Python 3.8+
- pandas
- sqlite3

See [`requirements.txt`](requirements.txt) for details.

## License

This project is provided for educational and