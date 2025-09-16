-- This query is to complete the Materials in Manufacturing order
-- It includes the manual JSON parsing of the CommentText field in the CommentText table
-- to extract its objects and values.
-- And it also includes how much material has been transferred to the MO
-- to be able to calculate how much material is still needed to complete the MO.
SELECT
    MOM.Id,
    MO.Id ManufacturingOrderId,
    MO.OrderNumber ManufacturingOrderNumber,
    MON.Id ManufacturingOrderNodeId,
    MON.Status ManufacturingOrderStatusId,
    CASE MON.Status
        WHEN 1 THEN 'Registered'
        WHEN 2 THEN 'Printed'
        WHEN 3 THEN 'Started'
        WHEN 4 THEN 'Finished'
        WHEN 5 THEN 'Post-calculated'
        WHEN 6 THEN 'Delivered'
        WHEN 9 THEN 'Historical'
    END ManufacturingOrderStatus,
    MOO.OperationNumber,
    Main_P.PartNumber MainPartNumber,
    Main_P.Description MainPartName,
    Main_P.ExtraDescription MainPartDescription,
    MOO.PlannedStartDate,
    MOO.PlannedFinishDate,
    MOO.ActualStartDate,
    MOO.ActualFinishDate,
    WC.Number WorkCenter,
    MOM.ReportNumber,
    Mat_P.PartNumber MaterialPartNumber,
    Mat_P.Description MaterialPartName,
    Mat_P.ExtraDescription MaterialDescription,
    COALESCE(Mat_PL.CurrentBalance, 0) MaterialCurrentBalance,
    MOM.PlannedQuantity MaterialPlannedQuantity,
    MOM.ReportedQuantity MaterialReportedQuantity,
    (MOM.PlannedQuantity - MOM.ReportedQuantity) MaterialOutstandingQuantity,
    MOM.RestQuantity MaterialRemainingQuantity,
    COALESCE(TransferredTransaction.TransferredQuantity, 0) MaterialTransferredQuantity,
    IF (MaterialTransferredQuantity - MaterialReportedQuantity > 0) THEN (MaterialTransferredQuantity - MaterialReportedQuantity) ELSE 0 END IF MaterialRestQuantity,
    Mat_P_U_DP.[Text] MaterialUnit
FROM monitor.ManufacturingOrderMaterial MOM
INNER JOIN monitor.Part Mat_P ON Mat_P.Id = MOM.PartId
LEFT OUTER JOIN monitor.PartUnitUsage Mat_PUU ON Mat_PUU.Id = Mat_P.StandardPartUnitUsageId
LEFT OUTER JOIN monitor.Unit Mat_P_U ON Mat_P_U.Id = Mat_PUU.UnitId
LEFT OUTER JOIN monitor.DynamicPhrase Mat_P_U_DP ON Mat_P_U_DP.Id = Mat_P_U.CodeId
LEFT OUTER JOIN (
    SELECT
        PL.PartId,
        SUM(PL.Balance) CurrentBalance
    FROM monitor.PartLocation PL
    WHERE 1 = 1
        AND PL.WarehouseId = 1
        AND PL.LifeCycleState <> 99
    GROUP BY PL.PartId
) Mat_PL ON Mat_PL.PartId = Mat_P.Id
INNER JOIN monitor.ManufacturingOrder MO ON MO.Id = MOM.ManufacturingOrderId
INNER JOIN monitor.ManufacturingOrderNode MON ON MON.Id = MOM.ManufacturingOrderNodeId
INNER JOIN monitor.Part Main_P ON Main_P.Id = MON.PartId
INNER JOIN monitor.ManufacturingOrderOperation MOO ON 1 = 1
    AND MOO.ManufacturingOrderId = MO.Id
    AND MOO.ManufacturingOrderNodeId = MON.Id
    AND MOO.OperationNumber = MOM.ToOperationNo
LEFT OUTER JOIN (
    SELECT
        IM.PartId,
        SUM(IM.BalanceChange) TransferredQuantity,
        IF (CHARINDEX('ManufacturingOrderNumber', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ManufacturingOrderNumber":"', CT.RawText)), (LEN('"ManufacturingOrderNumber":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ManufacturingOrderNumber":"', CT.RawText)), (LEN('"ManufacturingOrderNumber":"') + 1))) - 1)
            ELSE NULL
        END IF ManufacturingOrderNumber,
        IF (CHARINDEX('NodePartNumber', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"NodePartNumber":"', CT.RawText)), (LEN('"NodePartNumber":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"NodePartNumber":"', CT.RawText)), (LEN('"NodePartNumber":"') + 1))) - 1)
            ELSE NULL
        END IF NodePartNumber
    FROM monitor.InventoryMovement IM
    INNER JOIN monitor.CommentText CT ON 1 = 1
        AND CT.Id = IM.CommentId
        AND CT.RawText LIKE '{%}'
        AND CT.RawText LIKE '%"ManufacturingOrderNumber":"%'
        AND CT.RawText LIKE '%"NodePartNumber":"%'
    WHERE 1 = 1
        AND IM.BusinessTransactionContextType = 7
    GROUP BY
        IM.PartId,
        ManufacturingOrderNumber,
        NodePartNumber
) TransferredTransaction ON 1 = 1
    AND TransferredTransaction.PartId = Mat_P.Id
    AND TransferredTransaction.ManufacturingOrderNumber = MO.OrderNumber
    AND TransferredTransaction.NodePartNumber = Main_P.PartNumber
LEFT OUTER JOIN monitor.WorkCenter WC ON WC.Id = MOO.WorkCenterId
WHERE 1 = 1
    AND Mat_P.ExtraDescription NOT LIKE 'M-%'
    AND Mat_P.ExtraDescription NOT LIKE 'E-%'