-- This query retrieves the general ledger with adaptive opening and closing balances for a specified accounting year.
-- It includes details such as voucher information, account details, customer information, and calculates opening and closing balances.
SELECT
    CAST(ARL.Number AS VARCHAR) AccountsReceiveableJournalNumber,
    CAST(APL.Number AS VARCHAR) AccountsPayableJournalNumber,
    V.VoucherDate,
    V.Text VoucherText,
    CAST(AR.InvoiceNumber AS VARCHAR) InvoiceNumber,
    AR.ChineseVatInvoiceNumber EFakturNumber,
    CAST(V.Number AS VARCHAR) VoucherNumber,
    STRING(VS.Series, '-', V.Number) VoucherDescriptionNumber,
    CAST(A.Number AS VARCHAR) AccountNumber,
    DP_Account.[Text] AccountName,
    (
        CASE
            WHEN _VoucherRow.DebitInCompanyCurrency > 0 THEN (
                SELECT
                    LIST(DISTINCT A_Temp.Number, ', ')
                FROM monitor.VoucherRow VR
                INNER JOIN monitor.Balance B ON VR.BalanceId = B.Id
                LEFT OUTER JOIN monitor.Account A_Temp ON B.AccountId = A_Temp.Id
                WHERE 1 = 1
                    AND VR.VoucherId = V.Id
                    AND VR.Credit > 0
            )
            WHEN _VoucherRow.CreditInCompanyCurrency > 0 THEN (
                SELECT
                    LIST(DISTINCT A_Temp.Number, ', ')
                FROM monitor.VoucherRow VR
                INNER JOIN monitor.Balance B ON VR.BalanceId = B.Id
                LEFT OUTER JOIN monitor.Account A_Temp ON B.AccountId = A_Temp.Id
                WHERE 1 = 1
                    AND VR.VoucherId = V.Id
                    AND VR.Debit > 0
            )
            ELSE NULL
        END
    ) ContraAccountNumbers,
    C.Alias CustomerCode,
    CR.Name CustomerName,
    'IDR' CompanyCurrency,
    COALESCE(
        (
            SELECT
                SUM(OpeningBalancePeriod.OpeningBalance)
            FROM (
                SELECT
                    (OB.DebitInCompanyCurrency - OB.CreditInCompanyCurrency) OpeningBalance,
                    ROW_NUMBER() OVER (
                        PARTITION BY B_Temp.EntryIdentifier
                        ORDER BY FirstPeriodInYear.FromDate ASC
                    ) OpeningBalancePeriodIndex
                FROM monitor.Balance B_Temp
                LEFT OUTER JOIN monitor.OpeningBalance OB ON 1 = 1
                    AND B_Temp.OpeningBalanceId = OB.Id
                INNER JOIN (
                    SELECT
                        AYP.AccountingYearId,
                        MIN(AYP.FromDate) FromDate
                    FROM monitor.AccountingYearPeriod AYP
                    WHERE 1 = 1
                        AND DATEFORMAT(AYP.FromDate, 'YYYY') = :accounting_year
                    GROUP BY AYP.AccountingYearId
                ) FirstPeriodInYear ON 1 = 1
                    AND B_Temp.AccountingYearId = FirstPeriodInYear.AccountingYearId
                WHERE 1 = 1
                    AND B_Temp.AccountId = A.Id
            ) OpeningBalancePeriod
        )
    , 0) OpeningBalance,
    _VoucherRow.DebitInCompanyCurrency,
    _VoucherRow.CreditInCompanyCurrency,
    _VoucherRow.DifferenceInCompanyCurrency,
    SUM(_VoucherRow.DifferenceInCompanyCurrency) OVER (
        PARTITION BY A.Id
        ORDER BY V.VoucherDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) CumulativeTotalDifference,
    (OpeningBalance + CumulativeTotalDifference) ClosingBalance
FROM monitor.Voucher V
INNER JOIN monitor.VoucherSeries VS ON V.SeriesId = VS.Id
INNER JOIN monitor.AccountingYear AY ON V.AccountingYearId = AY.Id
OUTER APPLY (
    SELECT
        B.AccountId,
        SUM(VR.DebitInCompanyCurrency) DebitInCompanyCurrency,
        SUM(VR.CreditInCompanyCurrency) CreditInCompanyCurrency,
        (DebitInCompanyCurrency - CreditInCompanyCurrency) DifferenceInCompanyCurrency
    FROM monitor.VoucherRow VR
    INNER JOIN monitor.Balance B ON VR.BalanceId = B.Id
    WHERE 1 = 1
        AND VR.VoucherId = V.Id
    GROUP BY
        B.AccountId
) _VoucherRow
LEFT OUTER JOIN monitor.Account A ON _VoucherRow.AccountId = A.Id
LEFT OUTER JOIN monitor.AccountYearSetting AYS ON 1 = 1
    AND AYS.AccountingYearId = AY.Id
    AND AYS.ParentId = A.Id
LEFT OUTER JOIN monitor.DynamicPhrase DP_Account ON AYS.DescriptionId = DP_Account.Id
LEFT OUTER JOIN monitor.AccountsReceivableLedger ARL ON V.AccountsReceivableLedgerId = ARL.Id
LEFT OUTER JOIN monitor.AccountsPayableLedger APL ON V.AccountsPayableLedgerId = APL.Id
LEFT OUTER JOIN monitor.AccountsReceivable AR ON V.AccountsReceivableId = AR.Id
LEFT OUTER JOIN monitor.Customer C ON C.Id = AR.BusinessContactId
LEFT OUTER JOIN monitor.CustomerRoot CR ON C.RootId = CR.Id
WHERE 1 = 1
    AND V.IsPreliminary = 0
    AND AY.Description = (
        CASE 
            WHEN :accounting_year != '' OR :accounting_year IS NOT NULL THEN :accounting_year
            ELSE (SELECT Id FROM monitor.AccountingYear WHERE Status = 0)
        END
    )