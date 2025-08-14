PRAGMA foreign_keys = ON;

-- Borra las tablas existentes en orden inverso de dependencia
DROP TABLE IF EXISTS oil_production;
DROP TABLE IF EXISTS gas_production;
DROP TABLE IF EXISTS wells_by_county;

-- Tabla de Metadatos para los Pozos (Wells)
CREATE TABLE wells_by_county (
    id INTEGER PRIMARY KEY ,
    county TEXT NOT NULL,    
    operator INTEGER NOT NULL,
    status INTEGER NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL   

);

-- Tabla de Hechos para los Volúmenes de Producción Mensual
CREATE TABLE oil_production (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month Date,
    production REAL NULL,
    county TEXT NOT NULL -- ej. 'Thousand Barrels', 'MMcf'
);
CREATE TABLE gas_production (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month Date,
    production REAL NULL,
    county TEXT NOT NULL -- ej. 'Thousand Barrels', 'MMcf'
);

-- Índices para mejorar el rendimiento de las consultas
CREATE INDEX idx_oil_production_county_month ON oil_production(county, year_month);
CREATE INDEX idx_gas_production_county_month ON gas_production(county, year_month);
CREATE INDEX idx_wells_by_county_county ON wells_by_county(county);