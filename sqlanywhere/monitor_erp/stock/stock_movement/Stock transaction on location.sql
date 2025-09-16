-- This query is to complete the Stock transaction information on location
-- It includes the manual JSON parsing of the CommentText field in the CommentText table
-- to extract its objects and values.
SELECT DISTINCT
    P.PartNumber PartPartNumber,
    P.Type PartType,
    P.Id PartId,
    P.Description PartName,
    P.ExtraDescription PartDescription,
    IM.LoggingTimeStamp InventoryMovementLoggingTimeStamp,
    DATEFORMAT(IM.LoggingTimeStamp, 'hh:mm:ss') InventoryMovementLoggingTimeStampTimePart,
    IM.DeliveryDate InventoryMovementDeliveryDate,
    IM.LocationName InventoryMovementLocationName,
    IM.OrderNumber InventoryMovementOrderNumber,
    PO.OrderNumber PurchaseOrderOrderNumber,
    CO.OrderNumber CustomerOrderOrderNumber,
    COI.DeliveryNoteNumber DeliveryNoteNumber,
    CASE
        WHEN CT.ManufacturingOrderNumber IS NOT NULL THEN CT.ManufacturingOrderNumber
        ELSE MO.OrderNumber
    END ManufacturingOrderOrderNumber,
    CT.ReportNumber ManufacturingOrderReportNumber,
    IM.BusinessTransactionContextType TransactionTypeId,
    CASE IM.BusinessTransactionContextType
        WHEN 1 THEN 'Arrivals'
        WHEN 2 THEN 'Reporting manufacturing order as finished'
        WHEN 3 THEN 'Delivery'
        WHEN 4 THEN 'Stock count'
        WHEN 5 THEN 'Direct withdrawal'
        WHEN 6 THEN 'Move between warehouses'
        WHEN 7 THEN 'Move between locations'
        WHEN 8 THEN 'Case/Posterior injection'
        WHEN 9 THEN 'Register invoice directly'
        WHEN 10 THEN 'Location deleted'
        WHEN 11 THEN 'Material withdrawal for manufacturing order'
        WHEN 12 THEN 'Direct arrival'
        WHEN 13 THEN 'Balance import'
        WHEN 14 THEN 'Delivery (stock order)'
        WHEN 15 THEN 'Arrival (stock order)'
        WHEN 16 THEN 'Undone reporting - material'
        WHEN 17 THEN 'Undone reporting - finished part'
        WHEN 18 THEN 'Undone arrival'
        WHEN 19 THEN 'Undone delivery'
        WHEN 20 THEN 'Undone arrival - stock order'
        WHEN 21 THEN 'Undone delivery - stock order'
        WHEN 22 THEN 'Withdrawal (tools)'
        WHEN 23 THEN 'Return (tools)'
        WHEN 24 THEN 'Undone withdrawal (tools)'
        WHEN 25 THEN 'Undone return (tools)'
        WHEN 26 THEN 'Direct withdrawal (tools)'
        WHEN 27 THEN 'Direct return'
        WHEN 28 THEN 'Credit supplier invoice'
        WHEN 29 THEN 'Credit customer invoice'
        WHEN 30 THEN 'Return order'
        ELSE NULL
    END TransactionType,
    CASE 
        WHEN CT.CommentText IS NOT NULL THEN IF (CT.CommentText > '') THEN CT.CommentText ELSE NULL END IF
        WHEN CT.[Text] > '' THEN CT.[RawText]
        ELSE NULL
    END InventoryMovementComment,
    CT.[RawText] InventoryMovementCommentRawText,
    IM.UserName InventoryMovementUserName,
    CASE
        WHEN U.EmployeeNumber > '' THEN U.EmployeeNumber
        WHEN CT.EmployeeNumber > '' THEN CT.EmployeeNumber
        ELSE NULL
    END _UserEmployeeNumber,
    CASE
        WHEN U.EmployeeNumber > '' THEN STRING(U.FirstName, ' ', U.LastName)
        WHEN CT.EmployeeNumber > '' THEN CT.EmployeeFullName
        ELSE ''
    END _UserFullName,
    RCUSM.Code CauseCode,
    RCUSMDescriptionFallback.[Text] CauseCodeName,
    IM.BalanceChange / 1 BalanceChange,
    IM.SequenceNumber InventoryMovementSequenceNumber,
    EP_Part_GeneralPartNumber.[String] ExtraField_Part_GeneralPartNumber,
    EP_Part_PartClassification.[String] ExtraField_Part_PartClassification,
    IM.BalanceOnLocationAfterChange / 1 InventoryMovementBalanceOnLocationAfterChange,
    IM.BalanceOnPartAfterChange / 1 InventoryMovementBalanceOnPartAfterChange,
    CR.CustomerCode CustomerRootCustomerCodeAlias,
    SR.SupplierCode SupplierCodeAlias,
    PUU.ConversionFactor ConversionFactor,
    IM.UndoMovementId InventoryMovementUndoMovementId
FROM monitor.InventoryMovement IM
LEFT OUTER JOIN (
    SELECT
        CT.*,
        IF (CHARINDEX('"ManufacturingOrderNumber":"', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ManufacturingOrderNumber":"', CT.RawText)), (LEN('"ManufacturingOrderNumber":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ManufacturingOrderNumber":"', CT.RawText)), (LEN('"ManufacturingOrderNumber":"') + 1))) - 1)
            ELSE NULL
        END IF ManufacturingOrderNumber,
        IF (CHARINDEX('"ReportNumber":"', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ReportNumber":"', CT.RawText)), (LEN('"ReportNumber":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"ReportNumber":"', CT.RawText)), (LEN('"ReportNumber":"') + 1))) - 1)
            ELSE NULL
        END IF ReportNumber,
        IF (CHARINDEX('"EmployeeNumber":"', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"EmployeeNumber":"', CT.RawText)), (LEN('"EmployeeNumber":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"EmployeeNumber":"', CT.RawText)), (LEN('"EmployeeNumber":"') + 1))) - 1)
            ELSE NULL
        END IF EmployeeNumber,
        IF (CHARINDEX('"Comment":"', CT.RawText) > 0)
            THEN SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"Comment":"', CT.RawText)), (LEN('"Comment":"') + 1), CHARINDEX('"', SUBSTRING(SUBSTRING(CT.Text, CHARINDEX('"Comment":"', CT.RawText)), (LEN('"Comment":"') + 1))) - 1)
            ELSE NULL
        END IF CommentText,
        CASE WHEN EmployeeNumber > '' THEN STRING(Person.FirstName, ' ', Person.LastName) ELSE NULL END EmployeeFullName
    FROM monitor.CommentText CT
    LEFT OUTER JOIN monitor.Person Person ON EmployeeNumber = Person.EmployeeNumber
) CT ON CT.Id = IM.CommentId
LEFT OUTER JOIN monitor.Project Proj ON 1 = 1
    AND Proj.Id = IM.ProjectId
    AND Proj.Code = IM.ProjectNumber
LEFT OUTER JOIN monitor.Person U ON U.Id = IM.PersonId
LEFT OUTER JOIN monitor.Warehouse W ON W.Id = IM.WarehouseId
LEFT OUTER JOIN monitor.CustomerOrder CO ON 1 = 1
    AND CO.OrderNumber = IM.OrderNumber
    AND IM.BusinessTransactionContextType IN (3, 9, 14, 29)
LEFT OUTER JOIN monitor.CustomerOrderDeliveryRow CODR ON 1 = 1
    AND CODR.CustomerOrderId = CO.Id
    AND CODR.QuantityChangeId = IM.QuantityChangeId
LEFT OUTER JOIN monitor.CustomerOrderInvoice COI ON COI.Id = CODR.InvoiceId
LEFT OUTER JOIN monitor.PurchaseOrder PO ON 1 = 1
    AND PO.OrderNumber = IM.OrderNumber
    AND IM.BusinessTransactionContextType IN (1, 15)
LEFT OUTER JOIN monitor.ManufacturingOrder MO ON 1 = 1
    AND MO.OrderNumber = IM.OrderNumber
    AND IM.BusinessTransactionContextType IN (2, 11, 16, 17)
LEFT OUTER JOIN monitor.ProductRecord PR ON PR.SerialNumberWithPartNumber = IM.BatchNumber + ' - ' + IM.PartNumber
LEFT OUTER JOIN monitor.Customer C ON C.Id = CO.BusinessContactId
LEFT OUTER JOIN monitor.CustomerRoot CR ON CR.Id = C.RootId
LEFT OUTER JOIN monitor.Supplier S ON S.Id = PO.BusinessContactId
LEFT OUTER JOIN monitor.SupplierRoot SR ON SR.Id = S.RootId
LEFT OUTER JOIN monitor.Part P ON P.Id = IM.PartId
LEFT OUTER JOIN monitor.PartUnitUsage PUU ON PUU.Id = P.StandardPartUnitUsageId
LEFT OUTER JOIN monitor.ReasonCodeUnplannedStockMovement RCUSM ON RCUSM.Id = IM.ReasonCodeUnplannedStockMovementId
LEFT OUTER JOIN monitor.DynamicPhrase RCUSMDescriptionFallback ON RCUSM.DescriptionId = RCUSMDescriptionFallback.Id
LEFT OUTER JOIN monitor.ExtraField EP_Part_GeneralPartNumber ON 1 = 1
    AND EP_Part_GeneralPartNumber.ParentId = P.Id
    AND EP_Part_GeneralPartNumber.ParentClass = '6b6b98da-21a0-4ca4-9b88-21631c6ea572'
    AND EP_Part_GeneralPartNumber.TemplateId = 1028091941218534745
    AND EP_Part_GeneralPartNumber.[String] IS NOT NULL
LEFT OUTER JOIN monitor.ExtraField EP_Part_PartClassification ON 1 = 1
    AND EP_Part_PartClassification.ParentId = P.Id
    AND EP_Part_PartClassification.ParentClass = '6b6b98da-21a0-4ca4-9b88-21631c6ea572'
    AND EP_Part_PartClassification.TemplateId = 1028374453433595363
    AND EP_Part_PartClassification.[String] IS NOT NULL
LEFT OUTER JOIN monitor.InventoryMovement Undo_IM ON Undo_IM.Id = IM.UndoMovementId