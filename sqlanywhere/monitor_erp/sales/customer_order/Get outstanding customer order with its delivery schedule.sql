-- This query retrieves outstanding orders with its delivery schedule.
SELECT
    CO.Id CustomerOrderId,
    COR.Id CustomerOrderRowId,
    DS.Number DeliveryScheduleNumber,
    P.PartNumber,
    CO.OrderNumber,
    C.Alias CustomerCode,
    CR.Name CustomerName,
    CO.OrderDate,
    COALESCE(COR.CustomerOrderNumber, CO.BusinessContactOrderNumber) CustomerPurchaseOrderNumber,
    CO.Status OrderStatus,
    CASE CO.Status
        WHEN 0 THEN 'Not initialized'
        WHEN 1 THEN 'Registered'
        WHEN 2 THEN 'Printed'
        WHEN 3 THEN 'Delivery time cannot be confirmed'
        WHEN 4 THEN 'Picking in progress'
        WHEN 5 THEN 'Partial delivery made'
        WHEN 9 THEN 'Final delivery made'
        ELSE 'Unknown'
    END AS OrderStatusText,
    COR.Position,
    COR.CustomerCommitmentLevel,
    CASE COR.CustomerCommitmentLevel
        WHEN 1 THEN 'Fixed order'
        WHEN 2 THEN 'Manufacturing'
        WHEN 3 THEN 'Buy material'
        WHEN 4 THEN 'Forecast'
        ELSE 'Unknown'
    END CustomerCommitmentLevelText,
    COR.RowStatus,
    COR.Status RowOrderStatus,
    CASE COR.Status
        WHEN 0 THEN 'Not initialized'
        WHEN 1 THEN 'Registered'
        WHEN 2 THEN 'Printed'
        WHEN 3 THEN 'Delivery time cannot be confirmed'
        WHEN 4 THEN 'Picking in progress'
        WHEN 5 THEN 'Partial delivery made'
        WHEN 9 THEN 'Final delivery made'
        ELSE 'Unknown'
    END AS RowOrderStatusText,
    COR.DeliveryDate,
    CAST(DATEFORMAT(COR.DeliveryDate, 'YYYY-MM') AS VARCHAR(10)) DeliveryPeriod,
    COR.RestQuantity
FROM monitor.CustomerOrder CO
INNER JOIN monitor.CustomerOrderRow COR ON 1 = 1
    AND COR.ParentOrderId = CO.Id
    AND COR.LifeCycleState <> 99
    AND COR.Status <> 9
    AND COR.CustomerCommitmentLevel IN (1, 4)
INNER JOIN monitor.Part P ON 1 = 1
    AND P.Id = COR.PartId
    AND P.Status NOT IN (9)
LEFT OUTER JOIN monitor.Customer C ON C.Id = CO.BusinessContactId
LEFT OUTER JOIN monitor.CustomerRoot CR ON CR.Id = C.RootId
LEFT OUTER JOIN (
    SELECT
        DS.Id DeliveryScheduleId,
        EBT.Id EdiBusinessTransactionId,
        EFT.TransactionNumber EdiTransactionNumber,
        DS.Number,
        DS.BuyerCustomerCode,
        DS.BuyerCustomerId,
        EFT.FileName,
        EFT.FilePath,
        EFT.FileCreatedWhen,
        DSRCT.OrderNumber,
        DSRCT.Position,
        DSP.PartId,
        DSC.DeliveryDate,
        DSC.CommitmentLevel,
        DSC.DemandType,
        DSC.IsReplaced
    FROM monitor.DeliverySchedule DS
    INNER JOIN monitor.DeliverySchedulePart DSP ON DSP.ParentId = DS.Id
    INNER JOIN monitor.DeliveryScheduleReconciliationCommandTransferred DSRCT ON DSRCT.ParentId = DSP.Id
    INNER JOIN monitor.DeliveryScheduleCall DSC ON 1 = 1
        AND DSC.Id = DSRCT.CallId
        AND DSC.ParentId = DSP.Id
    LEFT OUTER JOIN monitor.EdiBusinessTransaction EBT ON EBT.BusinessEntityId = DS.Id
    LEFT OUTER JOIN monitor.EdiFileTransaction EFT ON EBT.FileTransactionId = EFT.Id
) DS ON 1 = 1
    AND (
        DS.BuyerCustomerCode = C.Alias OR DS.BuyerCustomerId = C.Id
    )
    AND (
        DS.DemandType = COR.CustomerCommitmentLevel OR DS.CommitmentLevel = COR.CustomerCommitmentLevel
    )
    AND DS.OrderNumber = CO.OrderNumber
    AND DS.PartId = P.Id
    AND DS.Position = COR.Position
    AND DS.DeliveryDate = COR.DeliveryDate
    AND DS.IsReplaced = 0
WHERE 1 = 1
    AND CO.Status NOT IN (9)
    AND CO.LifeCycleState <> 99
ORDER BY COR.DeliveryDate ASC