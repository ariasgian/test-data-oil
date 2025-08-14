-- Total oil and gas in West Virginia last 12 months
SELECT
    'Oil' AS product,
    SUM(production) AS total_volume
FROM oil_production
WHERE county = 'WV'
  AND year_month >= date('now', '-12 months')
UNION ALL
SELECT
    'Gas' AS product,
    SUM(production) AS total_volume
FROM gas_production
WHERE county = 'WV'
  AND year_month >= date('now', '-12 months');

-- County with the highest number of wells
SELECT county, COUNT(*) AS well_count
FROM wells_by_county
GROUP BY county
ORDER BY well_count DESC
LIMIT 1;

-- Average oil production per well
SELECT AVG(total_oil) AS avg_oil_per_well
FROM (
    SELECT year_month, SUM(production) AS total_oil
    FROM oil_production
    GROUP BY well_id
);

-- Average gas production per well
SELECT AVG(total_gas) AS avg_gas_per_well
FROM (
    SELECT year_month, SUM(production) AS total_gas
    FROM gas_production
    GROUP BY year_month
);
-- Bonus: Oil and gas production by year
SELECT
    product,
    year,
    total_volume,
    LAG(total_volume) OVER (PARTITION BY product ORDER BY year) AS prev_year_volume,
    ROUND(
        (CAST(total_volume AS REAL) - LAG(total_volume) OVER (PARTITION BY product ORDER BY year)) 
        / LAG(total_volume) OVER (PARTITION BY product ORDER BY year) * 100, 2
    ) AS yoy_change_percent
FROM (
    SELECT
        'Oil' AS product,
        strftime('%Y', year_month) AS year,
        SUM(production) AS total_volume
    FROM oil_production
    WHERE county = 'WV'
    GROUP BY year
    UNION ALL
    SELECT
        'Gas' AS product,
        strftime('%Y', year_month) AS year,
        SUM(production) AS total_volume
    FROM gas_production
    WHERE county = 'WV'
    GROUP BY year
)
ORDER BY product, year;