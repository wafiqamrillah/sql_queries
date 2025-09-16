-- This query generates a suggestion and actual order plan for a specified part number.
-- It uses a recursive CTE to generate a series of dates for the specified date range.
WITH RECURSIVE DateSeries (CurrentDate) AS (
    SELECT CAST('2025-03-01' AS DATE) -- Pastikan tipe awal adalah DATE
    UNION ALL
    SELECT CAST(DATEADD(DAY, 1, CurrentDate) AS DATE) -- CAST hasil DATEADD ke DATE
    FROM DateSeries
    WHERE CurrentDate < '2025-03-31'
),
MainPart AS (
    SELECT
        DateSeries.CurrentDate TransactionDate,
        P.*
    FROM monitor.Part P
    CROSS JOIN DateSeries
)
SELECT
    MainPart.TransactionDate,
    MainPart.Id PartId,
    MainPart.PartNumber,
    MainPart.Description PartName,
    MainPart.ExtraDescription PartDescription,
    CPL.RelativeRequirementNumber,
    CPL.PartType,
    CPL.MainPartId,
    CAST(COALESCE((
        SELECT
            SUM(COR.OrderedQuantity)
        FROM monitor.CustomerOrderRow COR
        INNER JOIN monitor.CustomerOrder CO ON 1 = 1
            AND COR.ParentOrderId = CO.Id
            AND CO.LifeCycleState <> 99
        WHERE 1 = 1
            AND COR.LifeCycleState <> 99
            AND COR.PartId = COALESCE(CPL.MainPartId, MainPart.Id)
            AND (
                (
                    CPL.IsMain = 1 OR CPL.IsParent = 1
                ) OR (
                    CPL.IsCustomerLink = 1
                    AND CO.BusinessContactId = CPL.CustomerId
                )
            )
            AND COR.DeliveryDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) OrderedQuantity,
    CAST(COALESCE((
        SELECT
            SUM(OOS.PlannedQuantity)
        FROM monitor.OrderOperationSuggestion OOS
        WHERE 1 = 1
            AND (
                OOS.PartId = MainPart.Id
                AND CPL.IsMain = 1
            )
            AND OOS.PlannedFinishDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) ManufacturingSuggestionQuantity,
    CAST(COALESCE((
        SELECT
            SUM(MOOR.Quantity)
        FROM monitor.ManufacturingOrderReporting MOR
        LEFT OUTER JOIN monitor.ManufacturingOrderOperationReporting MOOR ON MOR.Id = MOOR.ReportingId
        LEFT OUTER JOIN monitor.ManufacturingOrderNode MON ON MOR.RootNodeId = MON.Id
        WHERE 1 = 1
            AND MOR.ReportingType <> 6
            AND MOOR.Type IN (0,6,7,3,5,8,10,9,11,12)
            AND MOR.UndoManufacturingOrderReportingId IS NULL
            AND (
                CPL.IsMain = 1
                AND MON.PartId = MainPart.Id
            )
            AND MOOR.ActualReportedDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) ManufacturingQuantity,
    CAST(COALESCE((
        SELECT
            SUM(OS.OrderedQuantity)
        FROM monitor.OrderSuggestion OS
        WHERE 1 = 1
            AND OS.TransType = 3
            AND (
                OS.PartId = MainPart.Id
                AND CPL.IsMain = 1
            )
            AND OS.DeliveryDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) PurchasingSuggestionQuantity,
    CAST(COALESCE((
        SELECT
            SUM(QC.BalanceChange)
        FROM monitor.PurchaseOrderDeliveryRow PODR
        INNER JOIN monitor.PurchaseOrderDelivery POD ON POD.Id = PODR.PurchaseOrderDeliveryId
        LEFT OUTER JOIN monitor.PurchaseOrderRow POR ON PODR.PurchaseOrderRowId = POR.Id
        LEFT OUTER JOIN monitor.PurchaseOrderDeliveryRowQuantity PODRQ ON 1 = 1
            AND PODR.Id = PODRQ.ParentId
            AND PODRQ.BusinessTransactionContextType = 1
        INNER JOIN monitor.QuantityChange QC ON PODRQ.QuantityChangeId = QC.Id
        WHERE 1 = 1
            AND (
                CPL.IsMain = 1
                AND POR.PartId = MainPart.Id
            )
            AND PODR.DeliveryDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) ArrivalQuantity,
    CAST(COALESCE((
        SELECT
            SUM(QC.BalanceChange)
        FROM monitor.CustomerOrderInvoice COI
        LEFT OUTER JOIN monitor.CustomerOrderDeliveryRow CODR ON 1 = 1
            AND COI.Id = CODR.ParentInvoiceId
            AND CODR.ParentRowId IS NULL
        LEFT OUTER JOIN monitor.QuantityChange QC ON CODR.QuantityChangeId = QC.Id
        INNER JOIN monitor.CustomerOrderRow COR ON CODR.CustomerOrderRowId = COR.Id
        INNER JOIN monitor.CustomerOrder CO ON CODR.CustomerOrderId = CO.Id
        WHERE 1 = 1
            AND CODR.AffectStockBalance = 1
            AND QC.[Status] IN (3, 6)
            AND COR.PartId = COALESCE(CPL.MainPartId, MainPart.Id)
            AND (
                (
                    CPL.IsMain = 1 OR CPL.IsParent = 1
                ) OR (
                    CPL.IsCustomerLink = 1
                    AND CO.BusinessContactId = CPL.CustomerId
                )
            )
            AND CODR.DeliveryDate BETWEEN CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
    ), 0) AS DECIMAL(20, 0)) DeliveryQuantity,
    CAST(COALESCE((
        SELECT TOP 1
            IM.BalanceOnPartAfterChange
        FROM monitor.InventoryMovement IM
        WHERE 1 = 1
            AND (
                (
                    CPL.IsMain = 1 
                    AND IM.PartId = MainPart.Id
                )
                -- OR (
                --     CPL.IsParent = 1
                --     AND IM.PartId = CPL.MainPartId
                -- )
            )
            AND IM.LoggingTimeStamp <= DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(TransactionDate AS TIMESTAMP WITH TIME ZONE)))
        ORDER BY IM.LoggingTimeStamp DESC
    ), 0) AS DECIMAL(20, 0)) LastBalanceOnDay,
    OpeningStockCount.LoggingTimeStamp LastStockCountTakenDate,
    CAST(COALESCE((
        SELECT
            SUM(IM.BalanceChange) TotalTransaction
        FROM monitor.InventoryMovement IM
        WHERE 1 = 1
            AND IM.BusinessTransactionContextType <> 4
            AND IM.WarehouseId = 1
            AND (
                (
                    CPL.IsMain = 1
                    AND IM.PartId = MainPart.Id
                )
            )
            AND (
                (
                    IM.LoggingTimeStamp >= LastStockCountTakenDate
                    AND IM.DeliveryDate BETWEEN CAST(DATEFORMAT(IM.DeliveryDate, 'YYYY-MM-DD') AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(MainPart.TransactionDate AS TIMESTAMP WITH TIME ZONE)))
                    AND LastStockCountTakenDate IS NOT NULL
                ) OR (
                    1 = 1
                    AND IM.DeliveryDate BETWEEN CAST(MainPart.TransactionDate AS TIMESTAMP WITH TIME ZONE) AND DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(MainPart.TransactionDate AS TIMESTAMP WITH TIME ZONE)))
                    AND LastStockCountTakenDate IS NULL
                )
            )
    ), 0) AS DECIMAL(20, 0)) TotalTransactionFromStockCountUntilCurrent,
    CAST(
        CASE
            WHEN CPL.IsMain = 1 THEN ((COALESCE(OpeningStockCount.BalanceOnPartAfterChange, 0) + COALESCE(TotalTransactionFromStockCountUntilCurrent, 0)))
            ELSE 0
        END
    AS DECIMAL(20, 0)) ActualLastBalanceOnDay
FROM MainPart
LEFT OUTER JOIN (
    SELECT
        P.Id PartId,
        NULL RelativeRequirementNumber,
        NULL CustomerId,
        NULL MainPartId,
        'Main' PartType,
        1 IsMain,
        0 IsCustomerLink,
        0 IsParent
    FROM monitor.Part P
    UNION
    SELECT
        CPL.PartId,
        STRING(C.Alias, ' - ', CPL.CustomerPartNumber) RelativeRequirementNumber,
        CPL.CustomerId,
        NULL MainPartId,
        'CustomerLink' PartType,
        0 IsMain,
        1 IsCustomerLink,
        0 IsParent
    FROM monitor.CustomerPartLink CPL
    INNER JOIN monitor.Customer C ON CPL.CustomerId = C.Id
    UNION
    SELECT
        MR.PartId,
        P.PartNumber RelativeRequirementNumber,
        NULL CustomerId,
        P.Id MainPartId,
        'Parent' PartType,
        0 IsMain,
        0 IsCustomerLink,
        1 IsParent
    FROM monitor.MaterialRow MR
    INNER JOIN monitor.PartPreparation PP ON MR.PreparationId = PP.Id
    INNER JOIN monitor.Part P ON PP.PartId = P.Id
) CPL ON CPL.PartId = MainPart.Id
LEFT OUTER JOIN monitor.InventoryMovement OpeningStockCount ON 1 = 1
    AND OpeningStockCount.Id = (
        SELECT TOP 1
            Id
        FROM monitor.InventoryMovement IM
        WHERE 1 = 1
            AND IM.BusinessTransactionContextType = 4
            AND IM.PartId = MainPart.Id
            AND IM.DeliveryDate <= DATEADD(DAY, 1, DATEADD(MILLISECOND, -1, CAST(MainPart.TransactionDate AS TIMESTAMP WITH TIME ZONE)))
        ORDER BY IM.LoggingTimeStamp DESC
    )
WHERE 1 = 1
    AND MainPart.PartNumber IN (:PartNumber)
ORDER BY
    MainPart.TransactionDate ASC,
    MainPart.PartNumber ASC