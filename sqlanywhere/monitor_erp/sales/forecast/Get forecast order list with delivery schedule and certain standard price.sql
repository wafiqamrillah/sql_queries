SELECT *
FROM (
    (
        SELECT
            CR.CustomerCode,
            STRING(MAX(CR.CustomerCode), ' - ', MAX(CR.Name)) AS CustomerFullName,
            DSP.CustomerOrderNumber,
            P.PartNumber,
            MAX(P.Description) AS PartName,
            MAX(P.ExtraDescription) AS PartDescription,
            CPL.CustomerPartNumber,
            SUM(DSC.Quantity) AS PlannedQuantity,
            UnitCode.[Text] AS UnitCode,
            CustomerPrice.NewCurrencyCode AS CurrencyCode,
            CustomerPrice.PriceEach AS PriceEach,
            PriceEach * PlannedQuantity AS TotalPrice,
            DSC.DeliveryDate,
            MAX(CustomerPrice.UserCode) AS PriceChangeUserBy,
            CustomerPrice.PriceChangeDate AS PriceChangeDate,
            'EDI' AS CreatedBy
        FROM monitor.DeliverySchedule DS
        LEFT OUTER JOIN monitor.DeliveryScheduleType DST ON DST.Id = DS.DeliveryScheduleTypeId
        LEFT OUTER JOIN monitor.DynamicPhrase DSTDP ON DSTDP.Id = DST.DescriptionId
        LEFT OUTER JOIN monitor.DeliverySchedulePart DSP ON DS.Id = DSP.ParentId
        LEFT OUTER JOIN monitor.DeliveryScheduleCall DSC ON DSP.Id = DSC.ParentId
        LEFT OUTER JOIN monitor.Part P ON P.Id = DSP.PartId
        LEFT OUTER JOIN monitor.Customer C ON C.Id = DS.CustomerId
        LEFT OUTER JOIN monitor.CustomerRoot CR ON CR.Id = C.RootId
        LEFT OUTER JOIN monitor.CustomerPartLink CPL ON
            1=1
            AND P.Id = CPL.PartId
            AND C.Id = CPL.CustomerId
        LEFT OUTER JOIN monitor.Unit U ON U.Id = DSP.UnitId
        LEFT OUTER JOIN monitor.DynamicPhrase UnitCode ON UnitCode.Id = U.CodeId
        OUTER APPLY (
            SELECT TOP 1
                PCL.NewPrice AS PriceEach,
                Old_Cur.[Code] AS OldCurrencyCode,
                PCL.OldPrice,
                New_Cur.[Code] AS NewCurrencyCode,
                PCL.NewPrice,
                PCL.UserCode,
                PCL.[Timestamp] AS PriceChangeDate
            FROM monitor.PriceChangeLog PCL
            LEFT OUTER JOIN monitor.Currency Old_CUR ON Old_CUR.Id = PCL.OldPriceCurrencyId
            LEFT OUTER JOIN monitor.Currency New_CUR ON New_CUR.Id = PCL.NewPriceCurrencyId
            WHERE 1=1
                AND P.Id = PCL.PartId
                AND C.Id = PCL.CustomerId
                AND PCL.PriceType IN (-3, 3)
                AND PCL.[Timestamp] <= STRING(DATEFORMAT(DSC.DeliveryDate, 'yyyy-mm-dd'), 'T23:59:59.9999999+07:00')
                AND PCL.IsSetupPrice = 0
            ORDER BY
                PCL.[Timestamp] DESC
        ) AS CustomerPrice
        WHERE 1 = 1
            AND DSC.DemandType = 4
            AND DSC.DeliveryDate BETWEEN :FromDelDate AND :ToDelDate
            AND DS.CustomerDeliveryScheduleDate = (
                SELECT
                    MAX(DS_Temp.CustomerDeliveryScheduleDate) AS LastDate
                FROM monitor.DeliverySchedule DS_Temp
                LEFT OUTER JOIN monitor.DeliverySchedulePart DSP_Temp ON DS_Temp.Id = DSP_Temp.ParentId
                LEFT OUTER JOIN monitor.DeliveryScheduleCall DSC_Temp ON DSP_Temp.Id = DSC_Temp.ParentId
                WHERE 1 = 1
                    AND DSC_Temp.DemandType = 4
                    AND DS_Temp.CustomerId = DS.CustomerId
                    AND DSC_Temp.DeliveryDate BETWEEN :FromDelDate AND :ToDelDate
                    AND DSP_Temp.PartId = DSP.PartId
            )
        GROUP BY
            CR.CustomerCode,
            CR.Name,
            P.PartNumber,
            CPL.CustomerPartNumber,
            DSP.CustomerOrderNumber,
            DSC.DeliveryDate,
            UnitCode.[Text],
            CustomerPrice.NewCurrencyCode,
            CustomerPrice.PriceEach,
            CustomerPrice.PriceChangeDate
        ORDER BY
            CPL.CustomerPartNumber ASC,
            DSC.DeliveryDate ASC
    )
    UNION
    (
        SELECT
            CR.CustomerCode,
            STRING(CR.CustomerCode, ' - ', CR.Name) AS CustomerFullName,
            CO.BusinessContactOrderNumber AS CustomerOrderNumber,
            P.PartNumber,
            P.Description AS PartName,
            P.ExtraDescription AS PartDescription,
            CPL.CustomerPartNumber,
            COR.OrderedQuantity PlannedQuantity,
            UnitCode.[Text] AS UnitCode,
            'IDR' CurrencyCode,
            COR.Price * CO.ExchangeRate AS PriceEach,
            PriceEach * PlannedQuantity AS TotalPrice,
            COR.DeliveryDate,
            NULL AS PriceChangeUserBy,
            NULL AS PriceChangeDate,
            (
                SELECT TOP 1
                    EPL.ModifiedBy
                FROM monitor.EntityChangeLog EPL
                WHERE 1 = 1
                    AND EPL.EntityId = CO.Id
                    AND EPL.ChangeType = 1
                    AND EPL.EntityTypeId = '9A008BAB-1814-43EC-AFA5-4E42A5C3B36C'
            ) AS CreatedBy
        FROM monitor.CustomerOrder CO
        LEFT OUTER JOIN monitor.DeliveryScheduleReconciliationCommandTransferred DSRCT ON CO.OrderNumber = DSRCT.OrderNumber
        LEFT OUTER JOIN monitor.CustomerOrderRow COR ON COR.ParentOrderId = CO.Id
        LEFT OUTER JOIN monitor.Part P ON COR.PartId = P.Id
        INNER JOIN monitor.Currency Curr ON CO.CurrencyId = Curr.Id
        LEFT OUTER JOIN monitor.Customer C ON CO.BusinessContactId = C.Id
        LEFT OUTER JOIN monitor.CustomerRoot CR ON CR.Id = C.RootId
        LEFT OUTER JOIN monitor.CustomerPartLink CPL ON 1 = 1
            AND P.Id = CPL.PartId
            AND C.Id = CPL.CustomerId
        LEFT OUTER JOIN monitor.Unit U ON U.Id = COR.UnitId
        LEFT OUTER JOIN monitor.DynamicPhrase UnitCode ON UnitCode.Id = U.CodeId
        WHERE 1 = 1
            AND COR.CustomerCommitmentLevel = 1
            AND COR.WarehouseId = 1
            AND COR.CreationContext = 0
            AND COR.OrderRowType <> 4
            AND COR.PartId IS NOT NULL
            AND COR.FreeText IS NULL
            AND COR.LifeCycleState <> 99
            AND CO.LifeCycleState <> 99
            AND DSRCT.Id IS NULL
            AND COR.DeliveryDate BETWEEN :FromDelDate AND :ToDelDate
            AND 1 = :UseCOPlanning
    )
) SalesPlanTable