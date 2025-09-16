-- This query retrieves forecast data that came from delivery schedule.
SELECT
        DSC.Id DeliveryScheduleCallId,
        DS.Id DeliveryScheduleId,
        DS.Number DeliveryScheduleNumber,
        EBT.Id EdiBusinessTransactionId,
        EFT.TransactionNumber EdiTransactionNumber,
        EFT.FileName,
        EFT.FilePath,
        EFT.FileCreatedWhen,
        DS.Status DeliveryScheduleStatus,
        CASE DS.Status
            WHEN 0 THEN '0 - New'
            WHEN 1 THEN '1 - Activated'
            WHEN 3 THEN '3 - Reconciled'
            WHEN 5 THEN '5 - Transferred'
            WHEN 9 THEN '9 - Replaced'
            ELSE 'Unknown'
        END AS DeliveryScheduleStatusText,
        DSP.Status DeliverySchedulePartStatus,
        DSP.PreviousStatus DeliverySchedulePartPreviousStatus,
        DSP.ErrorTypes DeliverySchedulePartErrorTypes,
        DSP.WarningTypes DeliverySchedulePartWarningTypes,
        DS.BuyerCustomerCode CustomerCode,
        CO.OrderNumber CustomerOrderNumber,
        DSP.CustomerOrderNumber PurchaseOrderNumber,
        IF COR.Status >= 5 THEN 'Closed' ELSE 'Opened' END IF AS RowOrderStatus,
        DSP.PartId,
        P.PartNumber,
        P.Description PartName,
        P.ExtraDescription PartDescription,
        DSC.DemandType,
        DSC.Quantity,
        DSC.DeliveryDate,
        DSC.IsReplaced
    FROM monitor.DeliverySchedule DS
    LEFT OUTER JOIN monitor.DeliverySchedulePart DSP ON DSP.ParentId = DS.Id
    LEFT OUTER JOIN monitor.DeliveryScheduleReconciliationCommandTransferred DSRCT ON DSRCT.ParentId = DSP.Id
    INNER JOIN monitor.DeliveryScheduleCall DSC ON 1 = 1
        AND DSRCT.CallId = DSC.Id
        AND DSC.IsReplaced = 0
        AND (
            DSC.DemandType = 4 OR DSC.CommitmentLevel = 4
        ) -- Forecast type
    INNER JOIN monitor.Part P ON DSP.PartId = P.Id
    LEFT OUTER JOIN monitor.EdiBusinessTransaction EBT ON EBT.BusinessEntityId = DS.Id
    LEFT OUTER JOIN monitor.EdiFileTransaction EFT ON EBT.FileTransactionId = EFT.Id
    INNER JOIN monitor.Customer C ON 1 = 1
        AND (
            C.Alias = DS.BuyerCustomerCode OR C.Id = DS.BuyerCustomerId
        )
    INNER JOIN monitor.CustomerOrder CO ON 1 = 1
        AND C.Id = CO.BusinessContactId
        AND CO.OrderNumber = DSRCT.OrderNumber
        AND CO.LifeCycleState <> 99
    INNER JOIN monitor.CustomerOrderRow COR ON 1 = 1
        AND COR.ParentOrderId = CO.Id
        AND COR.LifeCycleState <> 99
        AND COR.PartId = DSP.PartId
        AND COR.DeliveryDate = DSC.DeliveryDate
        AND COR.CustomerCommitmentLevel = 4
    WHERE 1 = 1
        AND EBT.Id IS NOT NULL
        AND RowOrderStatus = 'Opened'
ORDER BY
    DSC.DeliveryDate ASC,
    EFT.FileCreatedWhen ASC,
    DS.Number ASC,
    P.PartNumber ASC,
    CO.OrderNumber ASC,
    COR.Position ASC