-- This query generates a yearly stock value report broken down by month
-- for each part and location. This query uses a recursive CTE to generate
-- a series of months for the specified year and joins it with inventory.
WITH RECURSIVE MonthSeries (CurrentDate) AS (
    SELECT 
        STRING(:Year, '-01') AS CurrentDate
    UNION ALL
    SELECT DATEFORMAT(DATEADD(MONTH, 1, STRING(CurrentDate, '-01')), 'YYYY-MM')
    FROM MonthSeries
    WHERE CurrentDate < STRING(:Year, '-12')
)
SELECT
    P.PartNumber,
    PL.LocationName,
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JAN' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'JanStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JAN' THEN EndStockCount.Amount ELSE 0 END) 'JanAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'FEB' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'FebStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'FEB' THEN EndStockCount.Amount ELSE 0 END) 'FebAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'MAR' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'MarStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'MAR' THEN EndStockCount.Amount ELSE 0 END) 'MarAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'APR' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'AprStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'APR' THEN EndStockCount.Amount ELSE 0 END) 'AprAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'MAY' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'MayStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'MAY' THEN EndStockCount.Amount ELSE 0 END) 'MayAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JUN' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'JunStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JUN' THEN EndStockCount.Amount ELSE 0 END) 'JunAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JUL' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'JulStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'JUL' THEN EndStockCount.Amount ELSE 0 END) 'JulAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'AUG' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'AugStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'AUG' THEN EndStockCount.Amount ELSE 0 END) 'AugAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'SEP' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'SepStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'SEP' THEN EndStockCount.Amount ELSE 0 END) 'SepAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'OCT' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'OctStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'OCT' THEN EndStockCount.Amount ELSE 0 END) 'OctAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'NOV' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'NovStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'NOV' THEN EndStockCount.Amount ELSE 0 END) 'NovAmounts',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'DEC' THEN EndStockCount.PhysicalInventoryBalance ELSE 0 END) 'DecStocks',
    SUM(CASE WHEN DATEFORMAT(MonthSeries.CurrentDate, 'MMM') = 'DEC' THEN EndStockCount.Amount ELSE 0 END) 'DecAmounts'
FROM MonthSeries
INNER JOIN monitor.Part P ON 1 = 1
    AND P.Status NOT IN (9)
    AND P.CategoryString NOT LIKE 'E%'
    AND P.CategoryString NOT LIKE 'M%'
    AND P.Type IN (0, 1)
INNER JOIN monitor.ProductGroup PG ON 1 = 1
    AND P.ProductGroupId = PG.Id
    AND (PG.Number != '024' OR PG.Number IS NULL)
INNER JOIN (
    SELECT DISTINCT
        PL_Temp.PartId,
        PL_Temp.LocationName,
        DATEFORMAT(PL_Temp.DeliveryDate, 'YYYY-MM') AS DeliveryMonth
    FROM monitor.InventoryMovement PL_Temp
    WHERE 1 = 1
        AND PL_Temp.WarehouseId = 1
        AND PL_Temp.DeliveryDate BETWEEN STRING(:Year, '-01-01T00:00:00.0000000+07:00') AND STRING(:Year, '-12-31T23:59:59.0000000+07:00')
        AND PL_Temp.BusinessTransactionContextType = 4
) PL ON 1 = 1
    AND PL.PartId = P.Id
    AND PL.DeliveryMonth = MonthSeries.CurrentDate
LEFT OUTER JOIN (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                IM_Temp.PartId,
                IM_Temp.LocationName,
                DATEFORMAT(IM_Temp.DeliveryDate, 'YYYY-MM')
            ORDER BY
                IM_Temp.DeliveryDate DESC,
                IM_Temp.LoggingTimeStamp DESC
        ) AS RowNumber,
        IM_Temp.*,
        DATEFORMAT(IM_Temp.DeliveryDate, 'YYYY-MM') AS DeliveryMonth,
        COALESCE(IM_Temp.PhysicalInventoryBalance, 0) * COALESCE(
            (
                SELECT TOP 1
                    PCL.NewPrice
                FROM monitor.PriceChangeLog PCL
                WHERE 1 = 1
                AND 1 = 1
                    AND PCL.PartId = IM_Temp.PartId
                    AND PCL.PriceType = 0
                    AND PCL.[Timestamp] <= STRING(DATEFORMAT(IM_Temp.DeliveryDate, 'YYYY-MM-DD'), 'T23:59:59.9999999+07:00')
                ORDER BY
                    PCL.[Timestamp] DESC
            ),
            0
        ) AS Amount
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.BusinessTransactionContextType = 4
        AND DATEFORMAT(IM_Temp.DeliveryDate, 'YYYY') = :Year
) EndStockCount ON 1 = 1
    AND EndStockCount.PartId = P.Id
    AND EndStockCount.LocationName = PL.LocationName
    AND EndStockCount.DeliveryMonth = MonthSeries.CurrentDate
    AND EndStockCount.RowNumber = 1
GROUP BY
    P.PartNumber,
    PL.LocationName
ORDER BY
    P.PartNumber ASC,
    PL.LocationName ASC