-- This query retrieves reported materials in manufacturing orders
-- that are not included in the Bill of Materials (BOM).
SELECT DISTINCT
    MOR.ReportedWhen,
    MO.OrderNumber,
    MainPart.PartNumber MainPartNumber,
    MainPart.Description MainPartName,
    MainPart.ExtraDescription MainPartDescription,
    MaterialPart.PartNumber MaterialPartNumber,
    MaterialPart.Description MaterialPartName,
    MaterialPart.ExtraDescription MaterialPartDescription
FROM monitor.ManufacturingOrderReporting MOR
INNER JOIN monitor.ManufacturingOrderMaterialReporting MOMR ON MOR.Id = MOMR.ReportingId
INNER JOIN monitor.ManufacturingOrderMaterial MOM ON MOMR.MaterialId = MOM.Id
INNER JOIN monitor.ManufacturingOrder MO ON MOR.OrderId = MO.Id
LEFT OUTER JOIN monitor.ManufacturingOrderNode MON ON MOM.ManufacturingOrderNodeId = MON.Id
INNER JOIN monitor.Part MainPart ON MON.PartId = MainPart.Id
INNER JOIN monitor.Part MaterialPart ON MOM.PartId = MaterialPart.Id
WHERE 1 = 1
    AND MOR.ReportedWhen BETWEEN '2025-04-01 00:00' AND '2025-04-25 23:59'
    AND MaterialPart.ExtraDescription NOT LIKE 'R-CRUSH%'
    AND MaterialPart.Id NOT IN (
        SELECT
            MR.PartId
        FROM monitor.MaterialRow MR
        INNER JOIN monitor.PartPreparation PP ON MR.PreparationId = PP.Id
        WHERE MainPart.PreparationId = PP.Id
    );