PRAGMA foreign_keys = ON;

-- Borra las tablas existentes en orden inverso de dependencia
DROP TABLE IF EXISTS production_volumes;
DROP TABLE IF EXISTS wells;
DROP TABLE IF EXISTS operators;
DROP TABLE IF EXISTS states;

-- Tabla de Dimensión para los Estados
CREATE TABLE states (
    state_id INTEGER PRIMARY KEY,
    state_name TEXT NOT NULL UNIQUE,
    state_code TEXT NOT NULL UNIQUE
);

-- Tabla de Dimensión para los Operadores
CREATE TABLE operators (
    operator_id INTEGER PRIMARY KEY,
    operator_name TEXT NOT NULL UNIQUE
);

-- Tabla de Metadatos para los Pozos (Wells)
CREATE TABLE wells (
    well_id INTEGER PRIMARY KEY,
    api_well_number TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('Active', 'Inactive', 'Plugged', 'Permitted')),
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    
    -- Claves foráneas para las relaciones
    state_id INTEGER NOT NULL,
    operator_id INTEGER NOT NULL,

    FOREIGN KEY(state_id) REFERENCES states(state_id),
    FOREIGN KEY(operator_id) REFERENCES operators(operator_id)
);

-- Tabla de Hechos para los Volúmenes de Producción Mensual
CREATE TABLE production_volumes (
    production_id INTEGER PRIMARY KEY,
    well_id INTEGER NOT NULL,
    production_date TEXT NOT NULL, -- Almacenar como texto en formato 'YYYY-MM-DD'
    product_type TEXT NOT NULL CHECK (product_type IN ('Oil', 'Gas')),
    volume REAL NOT NULL,
    unit TEXT NOT NULL, -- ej. 'Thousand Barrels', 'MMcf'
    
    FOREIGN KEY(well_id) REFERENCES wells(well_id),
    
    -- Restricción para asegurar que solo haya un registro por pozo, fecha y tipo de producto
    UNIQUE (well_id, production_date, product_type)
);

-- Índices para mejorar el rendimiento de las consultas
CREATE INDEX idx_wells_on_state_id ON wells(state_id);
CREATE INDEX idx_wells_on_operator_id ON wells(operator_id);
CREATE INDEX idx_production_on_well_id ON production_volumes(well_id);
CREATE INDEX idx_production_on_date ON production_volumes(production_date);
