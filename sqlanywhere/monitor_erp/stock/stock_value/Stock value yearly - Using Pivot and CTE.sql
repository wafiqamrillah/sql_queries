-- This query is designed to generate a pivot table of stock values
-- for each part and location, broken down by month for a specified year.
-- It uses a Common Table Expression (CTE) to create a series of months
-- for the specified year and joins it with inventory and compiles the data using PIVOT.
-- But, it cannot be used in Monitor ERP directly because of the PIVOT clause.
SELECT
    PartNumber,
    LocationName,
    COALESCE(['Jan'_stocks], 0) 'Jan_stocks',
    COALESCE(['Jan'_amounts], 0) 'Jan_amounts',
    COALESCE(['Feb'_stocks], 0) 'Feb_stocks',
    COALESCE(['Feb'_amounts], 0) 'Feb_amounts',
    COALESCE(['Mar'_stocks], 0) 'Mar_stocks',
    COALESCE(['Mar'_amounts], 0) 'Mar_amounts',
    COALESCE(['Apr'_stocks], 0) 'Apr_stocks',
    COALESCE(['Apr'_amounts], 0) 'Apr_amounts',
    COALESCE(['May'_stocks], 0) 'May_stocks',
    COALESCE(['May'_amounts], 0) 'May_amounts',
    COALESCE(['Jun'_stocks], 0) 'Jun_stocks',
    COALESCE(['Jun'_amounts], 0) 'Jun_amounts',
    COALESCE(['Jul'_stocks], 0) 'Jul_stocks',
    COALESCE(['Jul'_amounts], 0) 'Jul_amounts',
    COALESCE(['Aug'_stocks], 0) 'Aug_stocks',
    COALESCE(['Aug'_amounts], 0) 'Aug_amounts',
    COALESCE(['Sep'_stocks], 0) 'Sep_stocks',
    COALESCE(['Sep'_amounts], 0) 'Sep_amounts',
    COALESCE(['Oct'_stocks], 0) 'Oct_stocks',
    COALESCE(['Oct'_amounts], 0) 'Oct_amounts',
    COALESCE(['Nov'_stocks], 0) 'Nov_stocks',
    COALESCE(['Nov'_amounts], 0) 'Nov_amounts',
    COALESCE(['Dec'_stocks], 0) 'Dec_stocks',
    COALESCE(['Dec'_amounts], 0) 'Dec_amounts'
FROM (
    WITH RECURSIVE MonthSeries (CurrentDate) AS (
        SELECT 
            STRING('2025', '-01') AS CurrentDate
        UNION ALL
        SELECT DATEFORMAT(DATEADD(MONTH, 1, STRING(CurrentDate, '-01')), 'YYYY-MM')
        FROM MonthSeries
        WHERE CurrentDate < STRING('2025', '-12')
    )
    SELECT
        STRING(UPPER(LEFT(DATEFORMAT(MonthSeries.CurrentDate, 'MMM'), 1)), LOWER(SUBSTRING(DATEFORMAT(MonthSeries.CurrentDate, 'MMM'), 2))) AS CurrentMonth,
        P.PartNumber,
        PL.LocationName,
        COALESCE(EndStockCount.PhysicalInventoryBalance, 0) EndStock,
        COALESCE(EndStockCount.Amount, 0) Amount
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
            AND PL_Temp.DeliveryDate BETWEEN STRING('2025', '-01-01T00:00:00.0000000+07:00') AND STRING('2025', '-12-31T23:59:59.0000000+07:00')
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
            AND DATEFORMAT(IM_Temp.DeliveryDate, 'YYYY') = '2025'
    ) EndStockCount ON 1 = 1
        AND EndStockCount.PartId = P.Id
        AND EndStockCount.LocationName = PL.LocationName
        AND EndStockCount.DeliveryMonth = MonthSeries.CurrentDate
        AND EndStockCount.RowNumber = 1
) AS PIVOT_TABLE
PIVOT (
    SUM(EndStock) stocks,
    SUM(Amount) amounts
    FOR CurrentMonth IN (
        'Jan',
        'Feb',
        'Mar',
        'Apr',
        'May',
        'Jun',
        'Jul',
        'Aug',
        'Sep',
        'Oct',
        'Nov',
        'Dec'
    )
) PIVOT_MOVEMENT
ORDER BY PartNumber ASC