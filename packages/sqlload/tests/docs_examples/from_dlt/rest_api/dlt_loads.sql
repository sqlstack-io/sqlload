CREATE OR REPLACE VIEW shop._dlt_loads AS
SELECT
    _dlt_load_id AS load_id,
    'shop' AS schema_name,
    0 AS status,
    MIN(_sqlload_loaded_at) AS inserted_at
FROM (
    SELECT _dlt_load_id, _sqlload_loaded_at FROM shop.orders
    UNION ALL
    SELECT _dlt_load_id, _sqlload_loaded_at FROM shop.payments
) AS loads
GROUP BY _dlt_load_id;
