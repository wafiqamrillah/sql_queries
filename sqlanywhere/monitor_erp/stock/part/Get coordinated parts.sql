-- This query retrieves coordinated parts along with their main materials and coordinated materials.
-- It compares the quantities of the main and coordinated materials and flags any differences.
SELECT
    MainPart.Id MainPartId,
    MainPart.PartNumber MainPartNumber,
    MainPart.Description MainPartName,
    MainPart.ExtraDescription MainPartDescription,
    MainMaterial.PartNumber MainMaterialPartNumber,
    MainMaterial.PartDescription MainMaterialDescription,
    MainMaterial.Quantity MainMaterialPartQuantity,
    CoordinatedPart.Id CoordinatedPartId,
    CoordinatedPart.PartNumber CoordinatedPartNumber,
    CoordinatedPart.Description CoordinatedPartName,
    CoordinatedPart.ExtraDescription CoordinatedPartDescription,
    CoordinatedMaterial.PartNumber CoordinatedMaterialPartNumber,
    CoordinatedMaterial.PartDescription CoordinatedMaterialDescription,
    CoordinatedMaterial.Quantity CoordinatedMaterialPartQuantity,
    CASE WHEN MainMaterial.Quantity <> CoordinatedMaterial.Quantity THEN 1 ELSE 0 END IsDifferent
FROM monitor.CoordinatedPart CP
LEFT OUTER JOIN monitor.Part MainPart ON 1 = 1
    AND CP.PartId = MainPart.Id
OUTER APPLY (
    SELECT TOP 1
        P.PartNumber,
        P.Description PartName,
        P.ExtraDescription PartDescription,
        MR.Quantity
    FROM monitor.MaterialRow MR
    LEFT OUTER JOIN monitor.PartPreparation PP ON 1 = 1
        AND MR.PreparationId = PP.Id
    LEFT OUTER JOIN monitor.Part P ON 1 = 1
        AND MR.PartId = P.Id
    WHERE 1 = 1
        AND PP.PartId = MainPart.Id
        AND P.ExtraDescription LIKE 'R-%'
) MainMaterial
INNER JOIN monitor.CoordinatedPart CP2 ON 1 = 1
    AND CP2.CoordinatedPartHolderId = CP.CoordinatedPartHolderId
    AND CP2.PartId <> CP.PartId
INNER JOIN monitor.Part CoordinatedPart ON 1 = 1
    AND CoordinatedPart.Id = CP2.PartId
OUTER APPLY (
    SELECT TOP 1
        P.PartNumber,
        P.Description PartName,
        P.ExtraDescription PartDescription,
        MR.Quantity
    FROM monitor.MaterialRow MR
    LEFT OUTER JOIN monitor.PartPreparation PP ON 1 = 1
        AND MR.PreparationId = PP.Id
    LEFT OUTER JOIN monitor.Part P ON 1 = 1
        AND MR.PartId = P.Id
    WHERE 1 = 1
        AND PP.PartId = CoordinatedPart.Id
        AND P.ExtraDescription LIKE 'R-%'
) CoordinatedMaterial
WHERE 1 = 1