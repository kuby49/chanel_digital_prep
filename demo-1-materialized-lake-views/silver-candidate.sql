-- Candidate only: execute after the file-backed bronze schema and dimensions
-- have been validated. Replace each identifier token with a quoted, qualified
-- workspace.lakehouse.schema.object identifier.
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {{silver}}
(
    CONSTRAINT valid_fact CHECK (COALESCE(
        SalesLineID IS NOT NULL AND SalesLineID > 0 AND BatchID IS NOT NULL AND BatchID <> ''
        AND corrupt_record IS NULL
        AND SaleTimestamp IS NOT NULL AND CustomerID IS NOT NULL
        AND ProductID IS NOT NULL AND `Date` IS NOT NULL AND Quantity > 0
        AND UnitPriceMinorUnits >= 0 AND SalesAmountMinorUnits >= 0
        AND CAST(Quantity AS DECIMAL(38,0)) * CAST(UnitPriceMinorUnits AS DECIMAL(38,0))
            = CAST(SalesAmountMinorUnits AS DECIMAL(38,0))
        AND UnitPrice IS NOT NULL AND SalesAmount IS NOT NULL
        AND source_file IS NOT NULL
        AND payload_variants = 1 AND max_file_rows = 1 AND missing_source = 0
        AND CustomerName IS NOT NULL AND ProductName IS NOT NULL AND MonthName IS NOT NULL,
        FALSE
    )) ON MISMATCH FAIL
)
AS
SELECT
    s.SalesLineID, s.BatchID, s.corrupt_record, s.SaleTimestamp, s.CustomerID, s.ProductID,
    s.`Date`, s.Quantity, s.UnitPriceMinorUnits, s.SalesAmountMinorUnits,
    TRY_CAST(CAST(s.UnitPriceMinorUnits AS DECIMAL(38,0)) / 100 AS DECIMAL(18,2)) AS UnitPrice,
    TRY_CAST(CAST(s.SalesAmountMinorUnits AS DECIMAL(38,0)) / 100 AS DECIMAL(18,2)) AS SalesAmount,
    s.source_file, s.payload_variants, s.max_file_rows, s.missing_source,
    c.CustomerName, c.CustomerRegion,
    p.ProductName, p.ProductCategory, d.Year, d.MonthNumber, d.MonthName
FROM (
    SELECT
        d.SalesLineID, d.BatchID,
        TRY_CAST(d.payload.SaleTimestamp AS TIMESTAMP) AS SaleTimestamp,
        TRY_CAST(d.payload.CustomerID AS INT) AS CustomerID,
        TRY_CAST(d.payload.ProductID AS INT) AS ProductID,
        TRY_CAST(d.payload.`Date` AS DATE) AS `Date`,
        TRY_CAST(d.payload.Quantity AS INT) AS Quantity,
        TRY_CAST(d.payload.UnitPriceMinorUnits AS BIGINT) AS UnitPriceMinorUnits,
        TRY_CAST(d.payload.SalesAmountMinorUnits AS BIGINT) AS SalesAmountMinorUnits,
        d.payload.corrupt_record AS corrupt_record,
        d.source_file, d.payload_variants, d.max_file_rows, d.missing_source
    FROM (
        SELECT BatchID, SalesLineID, MIN(payload) AS payload,
            MIN(source_file) AS source_file,
            COUNT(DISTINCT payload) AS payload_variants,
            MAX(file_rows) AS max_file_rows,
            MAX(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS missing_source
        FROM (
            SELECT BatchID, TRY_CAST(SalesLineID AS BIGINT) AS SalesLineID,
                __filepath__ AS source_file,
                NAMED_STRUCT(
                    'raw_id', SalesLineID, 'SaleTimestamp', SaleTimestamp,
                    'CustomerID', CustomerID, 'ProductID', ProductID, 'Date', `Date`,
                    'Quantity', Quantity, 'UnitPriceMinorUnits', UnitPriceMinorUnits,
                    'SalesAmountMinorUnits', SalesAmountMinorUnits,
                    'corrupt_record', _corrupt_record
                ) AS payload,
                COUNT(*) AS file_rows
            FROM {{bronze}}
            GROUP BY BatchID, TRY_CAST(SalesLineID AS BIGINT), __filepath__,
                SalesLineID, SaleTimestamp, CustomerID, ProductID, `Date`, Quantity,
                UnitPriceMinorUnits, SalesAmountMinorUnits, _corrupt_record
        ) per_file
        GROUP BY BatchID, SalesLineID
    ) d
) s
LEFT JOIN {{dim_customer}} c ON s.CustomerID = c.CustomerID
LEFT JOIN {{dim_product}} p ON s.ProductID = p.ProductID
LEFT JOIN {{dim_date}} d ON s.`Date` = d.`Date`;
