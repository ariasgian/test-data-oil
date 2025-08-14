-- Total de petróleo (oil) y gas en West Virginia últimos 12 meses
SELECT
    'Oil' AS product,
    SUM(production) AS total_volume
FROM oil_production
WHERE state = 'WV'
  AND month >= date('now', '-12 months')
UNION ALL
SELECT
    'Gas' AS product,
    SUM(production) AS total_volume
FROM gas_production
WHERE state = 'WV'
  AND month >= date('now', '-12 months');

-- Condado con mayor número de pozos
SELECT location, COUNT(*) AS well_count
FROM wells_by_county
GROUP BY location
ORDER BY well_count DESC
LIMIT 1;

-- Promedio de producción de petróleo por pozo
SELECT AVG(total_oil) AS avg_oil_per_well
FROM (
    SELECT month, SUM(production) AS total_oil
    FROM oil_production
    GROUP BY well_id
);

-- Promedio de producción de gas por pozo
SELECT AVG(total_gas) AS avg_gas_per_well
FROM (
    SELECT month, SUM(production) AS total_gas
    FROM gas_production
    GROUP BY month
);