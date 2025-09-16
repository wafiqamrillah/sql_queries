-- This query retrieves forecast data that came from delivery schedule grouped by period.
SELECT
    DSC.FirstDeliveryScheduleCallId,
    DSC.DeliveryScheduleCallIds,
    DS.Number DeliveryScheduleNumber,
    EFT.TransactionNumber EdiTransactionNumber,
    C.Alias CustomerCode,
    CR.Name CustomerName,
    DSC.ForecastPeriod,
    EFT.FileName,
    EFT.FilePath,
    EFT.FileCreatedWhen
FROM monitor.DeliverySchedule DS
LEFT OUTER JOIN monitor.EdiBusinessTransaction EBT ON EBT.BusinessEntityId = DS.Id
LEFT OUTER JOIN monitor.EdiFileTransaction EFT ON EBT.FileTransactionId = EFT.Id
INNER JOIN monitor.Customer C ON 1 = 1
    AND (
        C.Alias = DS.BuyerCustomerCode OR C.Id = DS.BuyerCustomerId
    )
INNER JOIN monitor.CustomerRoot CR ON C.RootId = CR.Id
INNER JOIN (
    SELECT
        DSP.ParentId,
        CO.BusinessContactId,
        DATEFORMAT(DSC.DeliveryDate, 'YYYY-MM') ForecastPeriod,
        MIN(DSC.Id) FirstDeliveryScheduleCallId,
        LIST(DISTINCT STRING('''', DSC.Id, ''''), ', ') AS DeliveryScheduleCallIds
    FROM monitor.DeliveryScheduleCall DSC
    INNER JOIN monitor.DeliveryScheduleReconciliationCommandTransferred DSRCT ON DSRCT.CallId = DSC.Id
    INNER JOIN monitor.DeliverySchedulePart DSP ON DSRCT.ParentId = DSP.Id
    INNER JOIN monitor.Part P ON DSP.PartId = P.Id
    INNER JOIN monitor.CustomerOrder CO ON 1 = 1
        AND CO.OrderNumber = DSRCT.OrderNumber
        AND CO.LifeCycleState <> 99
        AND CO.Status <> 9
    INNER JOIN monitor.CustomerOrderRow COR ON 1 = 1
        AND COR.DeliveryDate = DSC.DeliveryDate
        AND COR.PartId = DSP.PartId
        AND COR.LifeCycleState <> 99
        AND COR.CustomerCommitmentLevel = 4
        AND COR.Status = 1
    WHERE 1 = 1
        AND DSC.IsReplaced = 0
        AND (
            DSC.DemandType = 4 OR DSC.CommitmentLevel = 4
        ) -- Forecast type
    GROUP BY
        DSP.ParentId,
        CO.BusinessContactId,
        ForecastPeriod
) DSC ON 1 = 1
    AND DSC.ParentId = DS.Id
    AND DSC.BusinessContactId = C.Id
WHERE 1 = 1
    AND EBT.Id IS NOT NULL
ORDER BY
    EFT.FileCreatedWhen ASC,
    ForecastPeriod ASC,
    DS.Number ASC