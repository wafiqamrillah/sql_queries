-- This query is used to predict the Delivery Note Number using CTEs
-- based on the stock movements within a specified date range
-- It considers created, replaced, and cancelled delivery notes
-- and uses window functions to fill in missing delivery note numbers
WITH InitDN AS (
    SELECT
        DATEFORMAT(IM.LoggingTimeStamp, 'YYYY-MM-DD hh:mm:ss') LoggingTime,
        MIN(IM.LoggingTimeStamp) LoggingTimeStamp,
        DATEFORMAT(UndoIM.LoggingTimeStamp, 'YYYY-MM-DD hh:mm:ss') UndoLoggingTime,
        MIN(UndoIM.LoggingTimeStamp) UndoLoggingTimeStamp,
        IM.UserName,
        UndoIM.UserName UndoUserName,
        CAST(COI.DeliveryNoteNumber AS VARCHAR(50)) DeliveryNoteNumber,
        LAST_VALUE(
            CAST(COI.DeliveryNoteNumber AS VARCHAR(50)) IGNORE NULLS
        ) OVER (
            ORDER BY LoggingTimeStamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) PreviousDeliveryNoteNumber,
        FIRST_VALUE(
            CAST(COI.DeliveryNoteNumber AS VARCHAR(50)) IGNORE NULLS
        ) OVER (
            ORDER BY LoggingTimeStamp ASC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) NextDeliveryNoteNumber,
        CAST(ReplacedInvoice.DeliveryNoteNumber AS VARCHAR(50)) ReplacedDeliveryNoteNumber,
        IM.DeliveryDate,
        CASE
            WHEN COI.DeliveryNoteNumber IS NOT NULL THEN 'Created'
            WHEN ReplacedInvoice.DeliveryNoteNumber IS NOT NULL THEN 'Replaced'
            ELSE 'Cancelled'
        END [Status],
        SUM (
            CASE WHEN COI.DeliveryNoteNumber IS NOT NULL THEN 1 ELSE 0 END
        ) OVER (
            ORDER BY LoggingTimeStamp ASC
        ) GroupId
    FROM monitor.InventoryMovement IM
    LEFT OUTER JOIN monitor.CustomerOrderInvoice COI ON 1 = 1
        AND COI.Id = (
            SELECT TOP 1
                CODR.InvoiceId
            FROM monitor.CustomerOrderDeliveryRow CODR
            WHERE 1 = 1
                AND CODR.QuantityChangeId = IM.QuantityChangeId
        )
    LEFT OUTER JOIN monitor.InventoryMovement UndoIM ON UndoIM.Id = IM.UndoMovementId
    LEFT OUTER JOIN (
        SELECT
            MIN(URLog.LoggingTimeStamp) LoggingTimeStamp,
            MIN(URLog.EntityIndentity) EntityIndentity,
            URLog.UserName
        FROM monitor.UndoReportingLog URLog
        WHERE URLog.[Type] = 1
        GROUP BY
            DATEFORMAT(URLog.LoggingTimeStamp, 'YYYY-MM-DD hh:mm:ss'),
            SUBSTRING(
                URLog.EntityIndentity,
                1,
                IF (CHARINDEX('/', URLog.EntityIndentity) > 0)
                    THEN (CHARINDEX('/', URLog.EntityIndentity)) - 1
                    ELSE LEN(URLog.EntityIndentity)
                ENDIF
            ),
            URLog.UserName
    ) URLog ON 1 = 1
        AND URLog.EntityIndentity LIKE STRING(IM.OrderNumber, '/%')
        AND DATEFORMAT(URLog.LoggingTimeStamp, 'YYYY-MM-DD hh:mm:ss') = DATEFORMAT(UndoIM.LoggingTimeStamp, 'YYYY-MM-DD hh:mm:ss')
        AND URLog.UserName = UndoIM.UserName
    LEFT OUTER JOIN monitor.CustomerOrderInvoice ReplacedInvoice ON ReplacedInvoice.OrderDeliveryNumber = URLog.EntityIndentity
    WHERE 1 = 1
        AND IM.BusinessTransactionContextType = 3
        AND IM.LoggingTimeStamp BETWEEN :StartDate AND :EndDate
    GROUP BY
        LoggingTime,
        UndoLoggingTime,
        IM.UserName,
        UndoIM.UserName,
        COI.DeliveryNoteNumber,
        ReplacedInvoice.DeliveryNoteNumber,
        IM.DeliveryDate
    ORDER BY
        LoggingTime ASC
),
DN AS (
    SELECT
        InitDN.*,
        CASE
            WHEN InitDN.DeliveryNoteNumber IS NULL THEN
                SUM(CASE WHEN InitDN.DeliveryNoteNumber IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY InitDN.GroupId ORDER BY InitDN.LoggingTimeStamp ASC)
            ELSE NULL
        END AscSumCount,
        CASE
            WHEN InitDN.DeliveryNoteNumber IS NULL THEN
                SUM(CASE WHEN InitDN.DeliveryNoteNumber IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY InitDN.GroupId ORDER BY InitDN.LoggingTimeStamp DESC)
            ELSE NULL
        END DescSumCount,
        CASE
            WHEN InitDN.PreviousDeliveryNoteNumber IS NOT NULL AND InitDN.DeliveryNoteNumber IS NULL THEN InitDN.PreviousDeliveryNoteNumber + AscSumCount
            WHEN InitDN.PreviousDeliveryNoteNumber IS NULL AND InitDN.DeliveryNoteNumber IS NULL THEN InitDN.NextDeliveryNoteNumber - DescSumCount
            ELSE InitDN.DeliveryNoteNumber
        END PredictDeliveryNoteNumber,
        STRING(CAST(PredictDeliveryNoteNumber AS DECIMAL(20, 0))) StringPredictDeliveryNoteNumber
    FROM InitDN
    ORDER BY
        InitDN.LoggingTimeStamp ASC
)
SELECT
    *
FROM DN