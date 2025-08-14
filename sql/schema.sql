PRAGMA foreign_keys = ON;

-- Borra las tablas existentes en orden inverso de dependencia
DROP TABLE IF EXISTS production_volumes;
DROP TABLE IF EXISTS wells;

-- Tabla de Metadatos para los Pozos (Wells)
CREATE TABLE wells (
    id INTEGER PRIMARY KEY,
    location TEXT NOT NULL,    
    operator INTEGER NOT NULL,
    status INTEGER NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL
    

);

-- Tabla de Hechos para los Volúmenes de Producción Mensual
CREATE TABLE production_volumes (
    id INTEGER NOT NULL,
    year_month Date,
    production REAL NULL,
    State TEXT NOT NULL, -- ej. 'Thousand Barrels', 'MMcf'
    
    FOREIGN KEY(id) REFERENCES wells(id),
    
    -- Restricción para asegurar que solo haya un registro por pozo, fecha y tipo de producto
    UNIQUE (id, year_month, State)
);

-- Índices para mejorar el rendimiento de las consultas
CREATE INDEX idx_wells_id ON wells(id);
CREATE INDEX idx_wells_operator ON wells(operator);
CREATE INDEX idx_production_id ON production_volumes(id);
CREATE INDEX idx_production_year_month ON production_volumes(year_month);