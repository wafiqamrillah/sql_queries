-- This query is designed to calculate stock of each part in the warehouse,
-- including the opening stock and end stock based on Stock count records, total of each transaction type, and how much quantity that back dated.
-- This version uses outer table joins.
SELECT
    P.PartNumber,
    P.Description PartName,
    P.ExtraDescription PartDescription,
    ExtraField_Part_1004537098651940025.[String] AS JobNumber,
    PL.LocationName,
    OpeningStockCount.PhysicalInventoryBalance OpeningStock,
    DATEFORMAT(OpeningStockCount.LoggingTimeStamp, 'YYYY-MM-DD HH:MM:SS') OpeningStockCountLoggingDate,
    DATEFORMAT(OpeningStockCount.DeliveryDate, 'YYYY-MM-DD HH:MM:SS') OpeningStockCountDate,
    COALESCE(TransactionMovementAfterOpening.ReceiptBalance, 0) BackDateReceiptBalance,
    COALESCE(TransactionMovementAfterOpening.IssueBalance, 0) BackDateIssueBalance,
    COALESCE(TransactionMovement.ReceiptBalance, 0) ReceiptBalance,
    COALESCE(TransactionMovement.IssueBalance, 0) IssueBalance,
    OpeningStock + BackDateReceiptBalance + BackDateIssueBalance + ReceiptBalance + IssueBalance EndStock,
    (COALESCE(EndStockCount.PhysicalInventoryBalance, (OpeningStock + BackDateReceiptBalance + BackDateIssueBalance + ReceiptBalance + IssueBalance)) - (OpeningStock + BackDateReceiptBalance + BackDateIssueBalance + ReceiptBalance + IssueBalance)) AS AdjustmentBalance,
    COALESCE(EndStockCount.PhysicalInventoryBalance, EndStock) ActualEndStock,
    DATEFORMAT(EndStockCount.LoggingTimeStamp, 'YYYY-MM-DD HH:MM:SS') EndStockCountLoggingDate,
    DATEFORMAT(EndStockCount.DeliveryDate, 'YYYY-MM-DD HH:MM:SS') EndStockCountDate,
    U_DP.[Text] UnitName
FROM monitor.Part P
LEFT OUTER JOIN monitor.PartCode PC ON 1 = 1
    AND PC.Id = P.PartCodeId
LEFT OUTER JOIN monitor.ProductGroup PG ON 1 = 1
    AND PG.Id = P.ProductGroupId
LEFT OUTER JOIN monitor.PartUnitUsage PUU ON 1 = 1
    AND PUU.Id = P.StandardPartUnitUsageId
LEFT OUTER JOIN monitor.Unit U ON 1 = 1
    AND U.Id = PUU.UnitId
LEFT OUTER JOIN monitor.DynamicPhrase U_DP ON 1 = 1
    AND U_DP.Id = U.CodeId
LEFT OUTER JOIN monitor.ExtraField AS ExtraField_Part_1004537098651940025 ON P.Id = ExtraField_Part_1004537098651940025.ParentId
	AND ExtraField_Part_1004537098651940025.ParentClass = '6b6b98da-21a0-4ca4-9b88-21631c6ea572'
	AND ExtraField_Part_1004537098651940025.TemplateId = 1004537098651940025
	AND ExtraField_Part_1004537098651940025.[String] IS NOT NULL
INNER JOIN (
    SELECT DISTINCT
        PL_Temp.PartId,
        PL_Temp.LocationName
    FROM monitor.InventoryMovement PL_Temp
    WHERE 1 = 1
        AND PL_Temp.WarehouseId = 1
        AND PL_Temp.DeliveryDate BETWEEN :Setting_StartDate AND :Setting_EndDate
) PL ON 1 = 1
    AND PL.PartId = P.Id
OUTER APPLY (
    SELECT TOP 1
        *
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.PartId = P.Id
        AND IM_Temp.LocationName = PL.LocationName
        AND IM_Temp.BusinessTransactionContextType = 4
        AND IM_Temp.DeliveryDate = DATEADD(DAY, -1, :Setting_StartDate)
    ORDER BY
        IM_Temp.LoggingTimeStamp DESC
) OpeningStockCount
OUTER APPLY (
    SELECT TOP 1
        *
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.PartId = P.Id
        AND IM_Temp.LocationName = PL.LocationName
        AND IM_Temp.BusinessTransactionContextType = 4
        AND IM_Temp.DeliveryDate = :Setting_EndDate
    ORDER BY
        IM_Temp.LoggingTimeStamp DESC
) EndStockCount
OUTER APPLY (
    SELECT
        SUM(
            CASE WHEN IM_Temp.BalanceChange > 0 THEN IM_Temp.BalanceChange ELSE 0 END
        ) ReceiptBalance,
        SUM(
            CASE WHEN IM_Temp.BalanceChange < 0 THEN IM_Temp.BalanceChange ELSE 0 END
        ) IssueBalance
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.PartId = P.Id
        AND IM_Temp.LocationName = PL.LocationName
        AND IM_Temp.BusinessTransactionContextType NOT IN (4)
        AND IM_Temp.DeliveryDate < :Setting_StartDate
        AND IM_Temp.LoggingTimeStamp >= OpeningStockCount.LoggingTimeStamp
) TransactionMovementAfterOpening
OUTER APPLY (
    SELECT
        SUM(
            CASE WHEN IM_Temp.BalanceChange > 0 THEN IM_Temp.BalanceChange ELSE 0 END
        ) ReceiptBalance,
        SUM(
            CASE WHEN IM_Temp.BalanceChange < 0 THEN IM_Temp.BalanceChange ELSE 0 END
        ) IssueBalance
    FROM monitor.InventoryMovement IM_Temp
    WHERE 1 = 1
        AND IM_Temp.BusinessTransactionContextType NOT IN (4)
        AND IM_Temp.PartId = P.Id
        AND IM_Temp.LocationName = PL.LocationName
        AND IM_Temp.DeliveryDate BETWEEN :Setting_StartDate AND :Setting_EndDate
        AND IM_Temp.LoggingTimeStamp <= EndStockCount.LoggingTimeStamp
) TransactionMovement
WHERE 1 = 1
    AND P.Status NOT IN (9)
    AND P.CategoryString NOT LIKE 'E%'
    AND P.CategoryString NOT LIKE 'M%'