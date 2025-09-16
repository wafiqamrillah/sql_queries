-- This query retrieves outstanding orders grouped by period and type.

SELECT
    P.PartNumber,
    P.Description PartName,
    P.ExtraDescription PartDescription,
    C.Alias CustomerCode,
    CR.Name CustomerName,
    COR.CustomerOrderRowCount,
    COR.CustomerCommitmentLevel,
    COR.CustomerCommitmentLevelText,
    COR.DeliveryPeriod,
    COR.RestQuantity
FROM monitor.Part P
INNER JOIN (
    SELECT
        MIN(COR.Id) FirstCustomerOrderRowId,
        LIST(DISTINCT COR.Id, ', ') CustomerOrderRowIds,
        COUNT(DISTINCT COR.Id) CustomerOrderRowCount,
        C.Id CustomerId,
        COR.CustomerCommitmentLevel,
        CASE COR.CustomerCommitmentLevel
            WHEN 1 THEN 'Fixed order'
            WHEN 2 THEN 'Manufacturing'
            WHEN 3 THEN 'Buy material'
            WHEN 4 THEN 'Forecast'
            ELSE 'Unknown'
        END CustomerCommitmentLevelText,
        COR.PartId,
        CAST(DATEFORMAT(COR.DeliveryDate, 'YYYY-MM') AS VARCHAR(10)) DeliveryPeriod,
        SUM(COR.RestQuantity) RestQuantity
    FROM monitor.CustomerOrderRow COR
    INNER JOIN monitor.CustomerOrder CO ON 1 = 1
        AND CO.Id = COR.ParentOrderId
        AND CO.LifeCycleState <> 99
    LEFT OUTER JOIN monitor.Customer C ON C.Id = CO.BusinessContactId
    WHERE 1 = 1
        AND COR.LifeCycleState <> 99
        AND COR.Status <> 9
        AND COR.CustomerCommitmentLevel IN (1, 4)
    GROUP BY
        C.Id,
        COR.CustomerCommitmentLevel,
        COR.PartId,
        DeliveryPeriod
) COR ON 1 = 1
    AND COR.PartId = P.Id
LEFT OUTER JOIN monitor.Customer C ON C.Id = COR.CustomerId
LEFT OUTER JOIN monitor.CustomerRoot CR ON CR.Id = C.RootId
WHERE 1 = 1
    AND P.Status NOT IN (9)