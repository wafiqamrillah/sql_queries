-- This query is designed to calculate stock of each part in the warehouse,
-- including the opening stock and end stock based on Stock count records, total of each transaction type, and how much quantity that back dated.
-- This version uses CTEs combined with subqueries and joins to optimize performance.
WITH OpeningStockCount AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY IM_Temp.PartId, IM_Temp.LocationName ORDER BY IM_Temp.LoggingTimeStamp DESC) AS RowNumber,
        IM_Temp.PartId,
        IM_Temp.LocationName,
        IM_Temp.PhysicalInventoryBalance,
        IM_Temp.LoggingTimeStamp,
        IM_Temp.DeliveryDate,
        COALESCE((
            SELECT
                SUM(
                    CASE WHEN IM_Temp2.BalanceChange > 0 THEN IM_Temp2.BalanceChange ELSE 0 END
                )
            FROM monitor.InventoryMovement IM_Temp2
            WHERE 1 = 1
                AND IM_Temp2.PartId = IM_Temp.PartId
                AND IM_Temp2.LocationName = IM_Temp.LocationName
                AND IM_Temp2.BusinessTransactionContextType NOT IN (4)
                AND IM_Temp2.DeliveryDate < :Setting_StartDate
                AND IM_Temp2.LoggingTimeStamp >= IM_Temp.LoggingTimeStamp
        ), 0) BackDateReceiptBalance,
        COALESCE((
            SELECT
                SUM(
                    CASE WHEN IM_Temp2.BalanceChange < 0 THEN IM_Temp2.BalanceChange ELSE 0 END
                )
            FROM monitor.InventoryMovement IM_Temp2
            WHERE 1 = 1
                AND IM_Temp2.PartId = IM_Temp.PartId
                AND IM_Temp2.LocationName = IM_Temp.LocationName
                AND IM_Temp2.BusinessTransactionContextType NOT IN (4)
                AND IM_Temp2.DeliveryDate < :Setting_StartDate
                AND IM_Temp2.LoggingTimeStamp >= IM_Temp.LoggingTimeStamp
        ), 0) BackDateIssueBalance
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.BusinessTransactionContextType = 4
        AND IM_Temp.DeliveryDate BETWEEN DATEADD(DAY, -7, :Setting_StartDate) AND :Setting_StartDate
),
EndStockCount AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY IM_Temp.PartId, IM_Temp.LocationName ORDER BY IM_Temp.LoggingTimeStamp DESC) AS RowNumber,
        IM_Temp.PartId,
        IM_Temp.LocationName,
        IM_Temp.PhysicalInventoryBalance,
        IM_Temp.LoggingTimeStamp,
        IM_Temp.DeliveryDate,
        COALESCE(
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
        ) AS StandardPrice,
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
        AND IM_Temp.DeliveryDate = :Setting_EndDate
)
SELECT
    P.PartNumber,
    P.Description PartName,
    P.ExtraDescription PartDescription,
    EF_Part_JobNumber.[String] AS JobNumber,
    PL.LocationName,
    MAX(OpeningStockCount.PhysicalInventoryBalance) / COALESCE(MAX(PUU.ConversionFactor), 1) OpeningStock,
    DATEFORMAT(MAX(OpeningStockCount.LoggingTimeStamp), 'YYYY-MM-DD HH:MM:SS') OpeningStockCountLoggingDate,
    DATEFORMAT(MAX(OpeningStockCount.DeliveryDate), 'YYYY-MM-DD HH:MM:SS') OpeningStockCountDate,
    MAX(OpeningStockCount.BackDateReceiptBalance) / COALESCE(MAX(PUU.ConversionFactor), 1) BackDateReceiptBalance,
    MAX(OpeningStockCount.BackDateIssueBalance) / COALESCE(MAX(PUU.ConversionFactor), 1) BackDateIssueBalance,
    SUM(
        CASE WHEN IM.BalanceChange > 0 THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) ReceiptBalance,
    SUM(
        CASE WHEN IM.BalanceChange < 0 THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) IssueBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (1, 18) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) ArrivalBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (2, 17) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) ReportedManufacturedBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (3, 19) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) DeliveryBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (11, 16) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) MaterialWithdrawalBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (6, 7) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) MovementBalance,
    SUM(
        CASE WHEN IM.BusinessTransactionContextType IN (5, 9, 12, 13) THEN IM.BalanceChange ELSE 0 END
    ) / COALESCE(MAX(PUU.ConversionFactor), 1) DirectReportedBalance,
    COALESCE(OpeningStock, 0) + COALESCE(BackDateReceiptBalance, 0) + COALESCE(BackDateIssueBalance, 0) + COALESCE(ReceiptBalance, 0) + COALESCE(IssueBalance, 0) EndStock,
    COALESCE(MAX(EndStockCount.PhysicalInventoryBalance), 0) / COALESCE(MAX(PUU.ConversionFactor), 1) ActualEndStock,
    (COALESCE(ActualEndStock, (COALESCE(OpeningStock, 0) + COALESCE(BackDateReceiptBalance, 0) + COALESCE(BackDateIssueBalance, 0) + COALESCE(ReceiptBalance, 0) + COALESCE(IssueBalance, 0))) - (COALESCE(OpeningStock, 0) + COALESCE(BackDateReceiptBalance, 0) + COALESCE(BackDateIssueBalance, 0) + COALESCE(ReceiptBalance, 0) + COALESCE(IssueBalance, 0))) AS AdjustmentBalance,
    'IDR' Currency,
    MAX(EndStockCount.StandardPrice) * COALESCE(MAX(PUU.ConversionFactor), 1) StandardPrice,
    MAX(EndStockCount.Amount) * COALESCE(MAX(PUU.ConversionFactor), 1) Amount,
    DATEFORMAT(MAX(EndStockCount.LoggingTimeStamp), 'YYYY-MM-DD HH:MM:SS') EndStockCountLoggingDate,
    DATEFORMAT(MAX(EndStockCount.DeliveryDate), 'YYYY-MM-DD HH:MM:SS') EndStockCountDate,
    U_DP.[Text] UnitName
FROM monitor.Part P
LEFT OUTER JOIN monitor.PartCode PC ON 1 = 1
    AND PC.Id = P.PartCodeId
LEFT OUTER JOIN monitor.ProductGroup PG ON 1 = 1
    AND PG.Id = P.ProductGroupId
LEFT OUTER JOIN monitor.PartUnitUsage PUU ON 1 = 1
    AND PUU.Id = P.StandardPartUnitUsageId
    AND PUU.PartId = P.Id
LEFT OUTER JOIN monitor.Unit U ON 1 = 1
    AND U.Id = PUU.UnitId
LEFT OUTER JOIN monitor.DynamicPhrase U_DP ON 1 = 1
    AND U_DP.Id = U.CodeId
LEFT OUTER JOIN monitor.ExtraField AS EF_Part_JobNumber ON P.Id = EF_Part_JobNumber.ParentId
	AND EF_Part_JobNumber.ParentClass = '6b6b98da-21a0-4ca4-9b88-21631c6ea572'
	AND EF_Part_JobNumber.TemplateId = 1004537098651940025
	AND EF_Part_JobNumber.[String] IS NOT NULL
INNER JOIN (
    SELECT DISTINCT
        PL_Temp.PartId,
        PL_Temp.LocationName
    FROM monitor.InventoryMovement PL_Temp
    WHERE PL_Temp.WarehouseId = 1
        AND PL_Temp.DeliveryDate BETWEEN :Setting_StartDate AND :Setting_EndDate
) PL ON PL.PartId = P.Id
LEFT OUTER JOIN OpeningStockCount ON 1 = 1
    AND OpeningStockCount.PartId = P.Id
    AND OpeningStockCount.LocationName = PL.LocationName
    AND OpeningStockCount.RowNumber = 1
LEFT OUTER JOIN EndStockCount ON 1 = 1
    AND EndStockCount.PartId = P.Id
    AND EndStockCount.LocationName = PL.LocationName
    AND EndStockCount.RowNumber = 1
LEFT OUTER JOIN monitor.InventoryMovement IM ON 1 = 1
    AND IM.BusinessTransactionContextType NOT IN (4)
    AND IM.PartId = P.Id
    AND IM.LocationName = PL.LocationName
    AND IM.DeliveryDate BETWEEN :Setting_StartDate AND :Setting_EndDate
    AND IM.LoggingTimeStamp <= COALESCE(EndStockCount.LoggingTimeStamp, :Setting_EndDate)
WHERE 1 = 1
    AND P.Status NOT IN (9)
    AND P.CategoryString NOT LIKE 'E%'
    AND P.CategoryString NOT LIKE 'M%'
GROUP BY
    P.Id,
    P.PartNumber,
    P.Description,
    P.ExtraDescription,
    EF_Part_JobNumber.[String],
    PL.LocationName,
    U_DP.[Text]