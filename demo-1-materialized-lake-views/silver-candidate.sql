-- Candidate only: execute after the file-backed bronze schema and dimensions
-- have been validated. Replace each identifier token with a quoted, qualified
-- workspace.lakehouse.schema.object identifier.
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {{silver}}
(
    CONSTRAINT valid_fact CHECK (COALESCE(
        SalesLineID IS NOT NULL AND BatchID IS NOT NULL AND BatchID <> ''
        AND SaleTimestamp IS NOT NULL AND CustomerID IS NOT NULL
        AND ProductID IS NOT NULL AND Date IS NOT NULL AND Quantity > 0
        AND UnitPriceMinorUnits >= 0 AND SalesAmountMinorUnits >= 0
        AND CAST(Quantity AS DECIMAL(38,0)) * CAST(UnitPriceMinorUnits AS DECIMAL(38,0))
            = CAST(SalesAmountMinorUnits AS DECIMAL(38,0))
        AND UnitPrice IS NOT NULL AND SalesAmount IS NOT NULL
        AND source_file IS NOT NULL
        AND CustomerName IS NOT NULL AND ProductName IS NOT NULL AND MonthName IS NOT NULL,
        FALSE
    )) ON MISMATCH FAIL
)
AS
SELECT
    s.SalesLineID, s.BatchID, s.SaleTimestamp, s.CustomerID, s.ProductID,
    s.Date, s.Quantity, s.UnitPriceMinorUnits, s.SalesAmountMinorUnits,
    TRY_CAST(CAST(s.UnitPriceMinorUnits AS DECIMAL(38,0)) / 100 AS DECIMAL(18,2)) AS UnitPrice,
    TRY_CAST(CAST(s.SalesAmountMinorUnits AS DECIMAL(38,0)) / 100 AS DECIMAL(18,2)) AS SalesAmount,
    s.source_file, c.CustomerName, c.CustomerRegion,
    p.ProductName, p.ProductCategory, d.Year, d.MonthNumber, d.MonthName
FROM (
    SELECT
        TRY_CAST(SalesLineID AS BIGINT) AS SalesLineID,
        BatchID,
        TRY_CAST(SaleTimestamp AS TIMESTAMP) AS SaleTimestamp,
        TRY_CAST(CustomerID AS INT) AS CustomerID,
        TRY_CAST(ProductID AS INT) AS ProductID,
        TRY_CAST(Date AS DATE) AS Date,
        TRY_CAST(Quantity AS INT) AS Quantity,
        TRY_CAST(UnitPriceMinorUnits AS BIGINT) AS UnitPriceMinorUnits,
        TRY_CAST(SalesAmountMinorUnits AS BIGINT) AS SalesAmountMinorUnits,
        __filepath__ AS source_file
    FROM {{bronze}}
) s
LEFT JOIN {{dim_customer}} c ON s.CustomerID = c.CustomerID
LEFT JOIN {{dim_product}} p ON s.ProductID = p.ProductID
LEFT JOIN {{dim_date}} d ON s.Date = d.Date;
