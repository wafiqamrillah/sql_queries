-- This query is to get reported material that below estimated quantity.
SELECT
    LastReporting.LastReportingDate,
    MO.OrderNumber,
    MO.Status,
    MainPart.PartNumber,
    MainPart.Description PartName,
    MainPart.ExtraDescription PartDescription,
    MOM.ToOperationNo,
    MOO.PlannedQuantity,
    MOO.ReportedQuantity,
    MOO.RejectedQuantity,
    MOO.RestQuantity,
    P.PartNumber MaterialPartNumber,
    P.Description MaterialPartName,
    P.ExtraDescription MaterialPartDescription,
    MOM.QuantityPerUnit,
    MOM.PlannedQuantity MaterialPlannedQuantity,
    (COALESCE(MOM.ReportedQuantity, 0) + COALESCE(MaterialCrushing.ReportedQuantity, 0)) MaterialReportedQuantity,
    MOM.RejectedQuantity MaterialRejectedQuantity,
    MOM.RestQuantity MaterialRestQuantity,
    (MOO.ReportedQuantity + MOO.RejectedQuantity) * MOM.QuantityPerUnit EstimatedReportedQuantity,
    MaterialReportedQuantity - EstimatedReportedQuantity DifferenceReportedQuantity,
    IF(MaterialReportedQuantity < EstimatedReportedQuantity) THEN 1 ELSE 0 END IF IsBelowEstimated,
    PL.Balance CurrentBalance,
    SUM(DifferenceReportedQuantity) OVER (
        PARTITION BY P.PartNumber
        ORDER BY LastReporting.LastReportingDate ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) CumulativeTotalDifferenceReportedQuantityPerMaterialPart,
    CurrentBalance + CumulativeTotalDifferenceReportedQuantityPerMaterialPart CurrentBalancePerMaterialPartAfterReported
FROM monitor.ManufacturingOrder MO
LEFT OUTER JOIN monitor.ManufacturingOrderNode MON ON 1 = 1
    AND MO.Id = MON.ManufacturingOrderId
LEFT OUTER JOIN monitor.Part MainPart ON 1 = 1
    AND MainPart.Id = MON.PartId
LEFT OUTER JOIN monitor.ManufacturingOrderMaterial MOM ON 1 = 1
    AND MO.Id = MOM.ManufacturingOrderId
    AND MOM.ManufacturingOrderNodeId = MON.Id
LEFT OUTER JOIN monitor.Part P ON 1 = 1
    AND MOM.PartId = P.Id
LEFT OUTER JOIN monitor.ManufacturingOrderOperation MOO ON 1 = 1
    AND MOO.ManufacturingOrderId = MO.Id
    AND MOO.OperationNumber = MOM.ToOperationNo
    AND MOO.ManufacturingOrderNodeId = MON.Id
OUTER APPLY (
    SELECT TOP 1
        MOR.ReportedWhen LastReportingDate
    FROM monitor.ManufacturingOrderReporting MOR
    WHERE 1 = 1
        AND MOR.OrderId = MO.Id
) LastReporting
LEFT OUTER JOIN monitor.PartLocation PL ON 1 = 1
    AND PL.PartId = P.Id
    AND PL.LifeCycleState <> 99
    AND PL.Name LIKE 'WHCC-PROD'
OUTER APPLY (
    SELECT TOP 1
        P.PartNumber,
        P.Description PartName,
        P.ExtraDescription PartDescription,
        MOM.*
    FROM monitor.ManufacturingOrderMaterial MOM_Temp
    LEFT OUTER JOIN monitor.Part MaterialPart ON 1 = 1
        AND MOM_Temp.PartId = MaterialPart.Id
    WHERE 1 = 1
        AND MOM_Temp.ManufacturingOrderId = MO.Id
        AND MOM_Temp.ManufacturingOrderNodeId = MON.Id
        AND MOM_Temp.Id <> MOM.Id
        AND MaterialPart.ExtraDescription LIKE 'R-CRUSH%'
) MaterialCrushing
WHERE 1 = 1
    AND P.ExtraDescription LIKE 'R-%'
    AND (
        1 = 1
        AND P.ExtraDescription NOT LIKE 'R-MB-%'
        AND P.ExtraDescription NOT LIKE 'R-CRUSH%'
    )
    AND LastReporting.LastReportingDate BETWEEN '2024-11-01T08:00:00+07:00' AND NOW()
    AND IsBelowEstimated = 1
    AND MOM.QuantityPerUnit <> 0
    AND MO.Status IN (4, 5, 6, 7, 8, 9)
ORDER BY
    P.PartNumber ASC,
    LastReporting.LastReportingDate ASC