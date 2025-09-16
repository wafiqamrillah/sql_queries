-- This query is used to match the movement transaction that comes source location to target location
-- includes the manual JSON in comment section for stock movements between locations in the Monitor ERP system.
SELECT
	IM.LoggingTimeStamp,
	P.PartNumber,
	P.Description,
	P.ExtraDescription,
	IM.BatchNumber BatchNumber,
	PR.SerialNumberWithPartNumber SerialNumber,
	IM.LocationName FromLocation,
	IM2.LocationName ToLocation,
	IM2.BalanceChange,
	IM.BalanceOnLocationAfterChange FromLocationBalance,
	IM2.BalanceOnLocationAfterChange ToLocationBalance,
	UnitCodeFallback.[Text] Unit,
	CASE
        WHEN Person.EmployeeNumber > '' THEN Person.EmployeeNumber
        WHEN JsonByCommentText.EmployeeNumber > '' THEN JsonByCommentText.EmployeeNumber
        ELSE NULL
    END EmployeeNumber,
	CASE
        WHEN Person.EmployeeNumber > '' THEN STRING(Person.FirstName, ' ', Person.LastName)
        WHEN JsonByCommentText.EmployeeNumber > '' THEN JsonByCommentText.EmployeeFullName
        ELSE NULL
    END UserFullName,
	IM.UserName UserName,
    CASE
        WHEN JsonByCommentText.CommentText IS NOT NULL THEN IF (JsonByCommentText.CommentText > '') THEN JsonByCommentText.CommentText ELSE NULL END IF
		WHEN JsonByCommentText.BaseCommentText > '' THEN JsonByCommentText.BaseCommentText
        ELSE NULL
    END CommentText,
	JsonByCommentText.ManufacturingOrderNumber,
	JsonByCommentText.ReportNumber,
    JsonByCommentText.BaseCommentText
FROM monitor.InventoryMovement IM
LEFT OUTER JOIN monitor.Part P ON P.Id = IM.PartId
LEFT OUTER JOIN monitor.InventoryMovement IM2 ON 1 = 1
	AND IM2.PartId = IM.PartId
	AND IM2.LoggingTimeStamp = IM.LoggingTimeStamp
	AND IM2.BusinessTransactionId = IM.BusinessTransactionId
	AND IM2.BalanceChange > 0
LEFT OUTER JOIN monitor.PartUnitUsage PUU ON P.StandardPartUnitUsageId = PUU.Id
LEFT OUTER JOIN monitor.Unit U ON PUU.UnitId = U.Id
LEFT OUTER JOIN monitor.DynamicPhrase UnitCodeFallback ON U.CodeId = UnitCodeFallback.Id
LEFT OUTER JOIN monitor.ProductRecord PR ON IM.BatchNumber + ' - ' + IM.PartNumber = PR.SerialNumberWithPartNumber
LEFT OUTER JOIN monitor.Person Person ON IM.PersonId = Person.Id
LEFT OUTER JOIN (
	SELECT
		CT.Id,
		IF (CHARINDEX('{', CT.RawText) > 0 AND CHARINDEX('}', CT.RawText) > 0) THEN 1 ELSE 0 END IF IsJson,
		CT.RawText AS BaseCommentText,
		
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

        CASE WHEN EmployeeNumber > '' THEN STRING(Person.FirstName, ' ', Person.LastName) ELSE NULL END AS EmployeeFullName
	FROM monitor.CommentText CT
    LEFT OUTER JOIN monitor.Person Person ON EmployeeNumber = Person.EmployeeNumber
	WHERE CHARINDEX('{', CT.RawText) > 0 AND CHARINDEX('}', CT.RawText) > 0
) JsonByCommentText ON 1 = 1
	AND JsonByCommentText.Id IN (IM.CommentId, IM2.CommentId)
WHERE 1 = 1
	AND IM.BusinessTransactionContextType = 7
	AND IM.BalanceChange < 0