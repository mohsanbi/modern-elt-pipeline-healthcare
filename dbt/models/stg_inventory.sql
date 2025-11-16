-- models/stg_inventory.sql
SELECT
    CAST(ItemID AS STRING) AS item_id,
    CAST(ItemName AS STRING) AS item_name,
    CAST(ItemDescription AS STRING) AS item_description,
    SAFE_CAST(REPLACE(CAST(UnitPrice AS STRING), '$', '') AS NUMERIC) AS unit_price,
    SAFE_CAST(StockQuantity AS INT64) AS stock_quantity
FROM
    {{ source('healthcare_data_raw', 'inventory') }}