import psycopg2
import pandas as pd
from pulldata import *
from datetime import datetime, timedelta

def establish_database_connection():
    conn = psycopg2.connect(
        dbname='MyDatabase',
        user='postgres',
        password='bobwashere',
        host='localhost',
        port=5432
    )
    return conn

# Table Creation
def create_general_table_if_not_exists(cur):
    create_general_table_query = """
    CREATE TABLE IF NOT EXISTS General (
        "Ticker.Exchange" VARCHAR(50),
        "Ticker.Date" TEXT PRIMARY KEY,
        Ticker VARCHAR(10),
        StockExchange VARCHAR(50),
        Name VARCHAR(100),
        Industry VARCHAR(50),
        Sector VARCHAR(50),
        LongDescription TEXT,
        Website VARCHAR(255),
        "52WeekHigh" NUMERIC(10, 2),
        "52WeekLow" NUMERIC(10, 2),
        Volume TEXT
    );
    """
    cur.execute(create_general_table_query)
    
def create_income_statement_table_if_not_exists(cur):
    create_income_statement_table_query = """
    CREATE TABLE IF NOT EXISTS incomestatement (
    "Ticker.Exchange" VARCHAR(50),
    "Ticker.Date" TEXT PRIMARY KEY,
    Ticker VARCHAR(10),
    StockExchange VARCHAR(50),
    Name VARCHAR(100),
    TotalRevenue TEXT,
    CostOfGoodsSoldInclDA TEXT,
    GrossProfit TEXT,
    SellingGeneralAdministrativeExclOther TEXT,
    OtherOperatingExpense TEXT,
    OperatingIncome TEXT,
    InterestExpense TEXT,
    UnusualExpense TEXT,
    NetIncomeBeforeTaxes TEXT,
    IncomeTaxes TEXT,
    ConsolidatedNetIncome TEXT,
    EPSRecurring TEXT,
    EPSBasicBeforeExtraordinaries TEXT,
    EPSDiluted TEXT,
    EBITDA TEXT,
    PriceToEarningsRatio TEXT,
    PriceToSalesRatio TEXT,
    GrossMargin TEXT,
    OperatingMargin TEXT,
    NetMargin TEXT,
    SharesOutstanding TEXT,
    MarketCapitalization TEXT
);
"""
    cur.execute(create_income_statement_table_query)
    
def create_balance_sheet_table_if_not_exists(cur):
    create_balance_sheet_table_query = """
CREATE TABLE IF NOT EXISTS balancesheet (
    "Ticker.Exchange" VARCHAR(50),
    "Ticker.Date" TEXT PRIMARY KEY,
    Ticker VARCHAR(10),
    StockExchange VARCHAR(50),
    Name VARCHAR(100),
    "TotalAssets" NUMERIC(10, 2),
    "CurrentAssets" NUMERIC(10, 2),
    "CashCashEquivalentsShortTermInvestments" NUMERIC(10, 2),
    "CashAndCashEquivalents" NUMERIC(10, 2),
    "OtherShortTermInvestments" NUMERIC(10, 2),
    "Receivables" NUMERIC(10, 2),
    "AccountsReceivable" NUMERIC(10, 2),
    "Inventory" NUMERIC(10, 2),
    "RawMaterials" NUMERIC(10, 2),
    "WorkInProcess" NUMERIC(10, 2),
    "FinishedGoods" NUMERIC(10, 2),
    "OtherInventories" NUMERIC(10, 2),
    "PrepaidAssets" NUMERIC(10, 2),
    "RestrictedCash" NUMERIC(10, 2),
    "OtherCurrentAssets" NUMERIC(10, 2),
    "TotalNonCurrentAssets" NUMERIC(10, 2),
    "NetPPE" NUMERIC(10, 2),
    "GrossPPE" NUMERIC(10, 2),
    "Properties" NUMERIC(10, 2),
    "LandAndImprovements" NUMERIC(10, 2),
    "MachineryFurnitureEquipment" NUMERIC(10, 2),
    "OtherProperties" NUMERIC(10, 2),
    "ConstructionInProgress" NUMERIC(10, 2),
    "Leases" NUMERIC(10, 2),
    "AccumulatedDepreciation" NUMERIC(10, 2),
    "GoodwillAndOtherIntangibleAssets" NUMERIC(10, 2),
    "Goodwill" NUMERIC(10, 2),
    "OtherIntangibleAssets" NUMERIC(10, 2),
    "NonCurrentNoteReceivables" NUMERIC(10, 2),
    "OtherNonCurrentAssets" NUMERIC(10, 2),
    "TotalLiabilitiesNetMinorityInterest" NUMERIC(10, 2),
    "CurrentLiabilities" NUMERIC(10, 2),
    "PayablesAndAccruedExpenses" NUMERIC(10, 2),
    "Payables" NUMERIC(10, 2),
    "AccountsPayable" NUMERIC(10, 2),
    "TotalTaxPayable" NUMERIC(10, 2),
    "CurrentAccruedExpenses" NUMERIC(10, 2),
    "InterestPayable" NUMERIC(10, 2),
    "CurrentProvisions" NUMERIC(10, 2),
    "CurrentDebtAndCapitalLeaseObligation" NUMERIC(10, 2),
    "CurrentDebt" NUMERIC(10, 2),
    "OtherCurrentBorrowings" NUMERIC(10, 2),
    "CurrentCapitalLeaseObligation" NUMERIC(10, 2),
    "CurrentDeferredLiabilities" NUMERIC(10, 2),
    "CurrentDeferredRevenue" NUMERIC(10, 2),
    "OtherCurrentLiabilities" NUMERIC(10, 2),
    "TotalNonCurrentLiabilitiesNetMinorityInterest" NUMERIC(10, 2),
    "LongTermProvisions" NUMERIC(10, 2),
    "LongTermDebtAndCapitalLeaseObligation" NUMERIC(10, 2),
    "LongTermDebt" NUMERIC(10, 2),
    "LongTermCapitalLeaseObligation" NUMERIC(10, 2),
    "NonCurrentDeferredLiabilities" NUMERIC(10, 2),
    "NonCurrentDeferredTaxesLiabilities" NUMERIC(10, 2),
    "NonCurrentDeferredRevenue" NUMERIC(10, 2),
    "NonCurrentAccruedExpenses" NUMERIC(10, 2),
    "PreferredSecuritiesOutsideStockEquity" NUMERIC(10, 2),
    "OtherNonCurrentLiabilities" NUMERIC(10, 2),
    "TotalEquityGrossMinorityInterest" NUMERIC(10, 2),
    "StockholdersEquity" NUMERIC(10, 2),
    "CapitalStock" NUMERIC(10, 2),
    "PreferredStock" NUMERIC(10, 2),
    "CommonStock" NUMERIC(10, 2),
    "AdditionalPaidInCapital" NUMERIC(10, 2),
    "RetainedEarnings" NUMERIC(10, 2),
    "GainsLossesNotAffectingRetainedEarnings" NUMERIC(10, 2),
    "OtherEquityAdjustments" NUMERIC(10, 2),
    "MinorityInterest" NUMERIC(10, 2),
    "TotalCapitalization" NUMERIC(10, 2),
    "CommonStockEquity" NUMERIC(10, 2),
    "CapitalLeaseObligations" NUMERIC(10, 2),
    "NetTangibleAssets" NUMERIC(10, 2),
    "WorkingCapital" NUMERIC(10, 2),
    "InvestedCapital" NUMERIC(10, 2),
    "TangibleBookValue" NUMERIC(10, 2),
    "TotalDebt" INT,
    "NetDebt" INT,
    "ShareIssued" INT,
    "OrdinarySharesNumber" INT

);

"""
    cur.execute(create_balance_sheet_table_query)
    
def create_cash_flow_table_if_not_exists(cur):
    create_cash_flow_table_query = """
    CREATE TABLE IF NOT EXISTS cashflow (
        "Ticker.Exchange" VARCHAR(50),
        "Ticker.Date" TEXT PRIMARY KEY,
        Ticker VARCHAR(10),
        StockExchange VARCHAR(50),
        Name VARCHAR(100),
        OperatingCashFlow TEXT,
        CashFlowFromContinuingOperatingActivities TEXT,
        NetIncomeFromContinuingOperations TEXT,
        OperatingGainsLosses TEXT,
        GainLossOnSaleOfPPE TEXT,
        NetForeignCurrencyExchangeGainLoss TEXT,
        DepreciationAmortizationDepletion TEXT,
        DepreciationAndAmortization TEXT,
        Depreciation TEXT,
        AssetImpairmentCharge TEXT,
        StockBasedCompensation TEXT,
        OtherNonCashItems TEXT,
        ChangeInWorkingCapital TEXT,
        ChangeInReceivables TEXT,
        ChangesInAccountReceivables TEXT,
        ChangeInInventory TEXT,
        ChangeInPrepaidAssets TEXT,
        ChangeInPayablesAndAccruedExpense TEXT,
        ChangeInOtherCurrentAssets TEXT,
        ChangeInOtherCurrentLiabilities TEXT,
        ChangeInOtherWorkingCapital TEXT,
        InvestingCashFlow TEXT,
        CashFlowFromContinuingInvestingActivities TEXT,
        CapitalExpenditureReported TEXT,
        NetPPEPurchaseAndSale TEXT,
        PurchaseOfPPE TEXT,
        NetIntangiblesPurchaseAndSale TEXT,
        PurchaseOfIntangibles TEXT,
        SaleOfIntangibles TEXT,
        NetBusinessPurchaseAndSale TEXT,
        PurchaseOfBusiness TEXT,
        SaleOfBusiness TEXT,
        NetInvestmentPurchaseAndSale TEXT,
        PurchaseOfInvestment TEXT,
        SaleOfInvestment TEXT,
        NetOtherInvestingChanges TEXT,
        FinancingCashFlow TEXT,
        CashFlowFromContinuingFinancingActivities TEXT,
        NetIssuancePaymentsOfDebt TEXT,
        NetLongTermDebtIssuance TEXT,
        LongTermDebtIssuance TEXT,
        LongTermDebtPayments TEXT,
        NetCommonStockIssuance TEXT,
        CommonStockIssuance TEXT,
        ProceedsFromStockOptionExercised TEXT,
        NetOtherFinancingCharges TEXT,
        EndCashPosition TEXT,
        ChangesInCash TEXT,
        EffectOfExchangeRateChanges TEXT,
        BeginningCashPosition TEXT,
        IncomeTaxPaidSupplementalData TEXT,
        InterestPaidSupplementalData TEXT,
        CapitalExpenditure TEXT,
        IssuanceOfCapitalStock TEXT,
        IssuanceOfDebt TEXT,

        RepaymentOfDebt TEXT,
        FreeCashFlow TEXT
    );
    """
    cur.execute(create_cash_flow_table_query)

# Modify table with data
def update_or_insert_general_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM General WHERE 'Ticker.Date' = %s;"
    cur.execute(check_query, (ticker,))
    count = cur.fetchone()[0]

    if count > 0:
        update_query = """
        UPDATE General
        SET "Ticker.Date" = %s, "Ticker.Exchange" = %s, StockExchange = %s, Name = %s, Industry = %s, Sector = %s, 
            LongDescription = %s, Website = %s, "52WeekHigh" = %s, "52WeekLow" = %s, 
            Volume = %s
        WHERE Ticker = %s;
        """
        cur.execute(update_query, (
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            row['Industry'],
            row['Sector'],
            row['LongDescription'],
            row['Website'],
            row['52WeekHigh'],
            row['52WeekLow'],
            str(row['Volume']),
            ticker
        ))
    else:
        insert_query = """
        INSERT INTO General ("Ticker.Date", "Ticker.Exchange", Ticker, StockExchange, Name, Industry, Sector, LongDescription, Website, "52WeekHigh", "52WeekLow", Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(insert_query, ( 
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            ticker,
            row['StockExchange'],
            row['Name'],
            row['Industry'],
            row['Sector'],
            row['LongDescription'],
            row['Website'],
            row['52WeekHigh'],
            row['52WeekLow'],
            str(row['Volume'])
        ))
        
def update_or_insert_income_statement_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM incomestatement WHERE 'Ticker.Date' = %s;"
    cur.execute(check_query, (ticker,))
    count = cur.fetchone()[0]

    if count > 0:
        update_query = """
        UPDATE incomestatement
        SET "Ticker.Date" = %s, "Ticker.Exchange" = %s, "StockExchange" = %s, Name = %s, TotalRevenue = %s, GrossProfit = %s,
        SellingGeneralAdministrativeExclOther = %s, OtherOperatingExpense = %s, OperatingIncome = %s, InterestExpense = %s, UnusualExpense = %s, NetIncomeBeforeTaxes = %s, IncomeTaxes = %s,
        ConsolidatedNetIncome = %s, EPSRecurring = %s, EPSBasicBeforeExtraordinaries = %s, EPSDiluted = %s, EBITDA = %s;
        PriceToEarningsRatio = %s, PriceToSalesRatio = %s, GrossMargin = %s, OperatingMargin = %s,
        NetMargin = %s, SharesOutstanding = %s, MarketCapitalization = %s WHERE Ticker = %s;
        """
        values = [   
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            row['TotalRevenue'],
            row['GrossProfit'],
            row['SellingGeneralAdministrativeExclOther'],
            row['OtherOperatingExpense'],
            row['OperatingIncome'],
            row['InterestExpense'],
            row['UnusualExpense'],
            row['NetIncomeBeforeTaxes'],
            row['IncomeTaxes'],
            row['ConsolidatedNetIncome'],
            row['EPSRecurring'],
            row['EPSBasicBeforeExtraordinaries'],
            row['EPSDiluted'],
            row['EBITDA'],
            row['PriceToEarningsRatio'],
            row['PriceToSalesRatio'],
            row['GrossMargin'],
            row['OperatingMargin'],
            row['NetMargin'],
            row['SharesOutstanding'],
            row['MarketCapitalization'],
            ticker]
        cur.execute(update_query, values)
    else:
        insert_query = """
        INSERT INTO incomestatement (Ticker, 
        "Ticker.Date", "Ticker.Exchange", StockExchange, Name, TotalRevenue, GrossProfit, SellingGeneralAdministrativeExclOther,
        OtherOperatingExpense, OperatingIncome,InterestExpense, UnusualExpense, NetIncomeBeforeTaxes, IncomeTaxes,
        ConsolidatedNetIncome, EPSRecurring, EPSBasicBeforeExtraordinaries, EPSDiluted, EBITDA, PriceToEarningsRatio,
        PriceToSalesRatio, GrossMargin, OperatingMargin, NetMargin, SharesOutstanding, MarketCapitalization
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
        values = [
            ticker,
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            int(row['TotalRevenue']) if row['TotalRevenue'] is not None else None, 
            int(row['GrossProfit']) if row['GrossProfit'] is not None else None, 
            int(row['SellingGeneralAdministrativeExclOther']) if row['SellingGeneralAdministrativeExclOther'] is not None else None, 
            int(row['OtherOperatingExpense']) if row['OtherOperatingExpense'] is not None else None, 
            int(row['OperatingIncome']) if row['OperatingIncome'] is not None else None, 
            int(row['InterestExpense']) if row['InterestExpense'] is not None else None, 
            int(row['UnusualExpense']) if row['UnusualExpense'] is not None else None,
            int(row['NetIncomeBeforeTaxes']) if row['NetIncomeBeforeTaxes'] is not None else None, 
            int(row['IncomeTaxes']) if row['IncomeTaxes'] is not None else None,
            int(row['ConsolidatedNetIncome']) if row['ConsolidatedNetIncome'] is not None else None, 
            int(row['EPSRecurring']) if row['EPSRecurring'] is not None else None,
            int(row['EPSBasicBeforeExtraordinaries']) if row['EPSBasicBeforeExtraordinaries'] is not None else None, 
            int(row['EPSDiluted']) if row['EPSDiluted'] is not None else None,      
            int(row['EBITDA']) if row['EBITDA'] is not None else None,
            str(row['PriceToEarningsRatio']) if row['PriceToEarningsRatio'] is not None else None,         
            int(row['PriceToSalesRatio']) if row['PriceToSalesRatio'] is not None else None,
            int(row['GrossMargin']) if row['GrossMargin'] is not None else None,
            int(row['OperatingMargin']) if row['OperatingMargin'] is not None else None, 
            int(row['NetMargin']) if row['NetMargin'] is not None else None,    
            int(row['SharesOutstanding']) if row['SharesOutstanding'] is not None else None,  
            int(row['MarketCapitalization']) if row['MarketCapitalization'] is not None else None
        ]

    # Execute the query with the adapted values
        cur.execute(insert_query, values)
        
def update_or_insert_balance_sheet_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM balancesheet WHERE 'Ticker.Date' = %s;"
    cur.execute(check_query, (ticker,))
    count = cur.fetchone()[0]

    if count > 0:
        update_query = """
        UPDATE balancesheet
        SET "Ticker.Date" = %s, "Ticker.Exchange" = %s, "StockExchange" = %s, 
        Name = %s, "TotalAssets" = %s, "CurrentAssets" = %s, 
        "CashCashEquivalentsShortTermInvestments" = %s, "CashAndCashEquivalents" = %s, "OtherShortTermInvestments" = %s,
        "Receivables" = %s, "AccountsReceivable" = %s, "Inventory" = %s, 
        "RawMaterials" = %s, "WorkInProcess" = %s, "FinishedGoods" = %s, 
        "OtherInventories" = %s, "PrepaidAssets" = %s, "RestrictedCash" = %s, 
        "OtherCurrentAssets" = %s, "TotalNonCurrentAssets" = %s, "NetPPE" = %s, 
        "GrossPPE" = %s, "Properties" = %s, "LandAndImprovements" = %s,
        "MachineryFurnitureEquipment" = %s, "OtherProperties" = %s, "ConstructionInProgress" = %s,
        "Leases" = %s, "AccumulatedDepreciation" = %s, "GoodwillAndOtherIntangibleAssets" = %s,
        "Goodwill" = %s, "OtherIntangibleAssets" = %s, "NonCurrentNoteReceivables" = %s,
        "OtherNonCurrentAssets" = %s, "TotalLiabilitiesNetMinorityInterest" = %s, "CurrentLiabilities" = %s,
        "PayablesAndAccruedExpenses" = %s, "Payables" = %s, "AccountsPayable" = %s,
        "TotalTaxPayable" = %s, "CurrentAccruedExpenses" = %s, "InterestPayable" = %s,
        "CurrentProvisions" = %s, "CurrentDebtAndCapitalLeaseObligation" = %s, "CurrentDebt" = %s,
        "OtherCurrentBorrowings" = %s, "CurrentCapitalLeaseObligation" = %s, "CurrentDeferredLiabilities" = %s,
        "CurrentDeferredRevenue" = %s, "OtherCurrentLiabilities" = %s, "TotalNonCurrentLiabilitiesNetMinorityInterest" = %s,
        "LongTermProvisions" = %s, "LongTermDebtAndCapitalLeaseObligation" = %s, "LongTermDebt" = %s,
        "LongTermCapitalLeaseObligation" = %s, "NonCurrentDeferredLiabilities" = %s, "NonCurrentDeferredTaxesLiabilities" = %s,
        "NonCurrentDeferredRevenue" = %s, "NonCurrentAccruedExpenses" = %s, "PreferredSecuritiesOutsideStockEquity" = %s,
        "OtherNonCurrentLiabilities" = %s, "TotalEquityGrossMinorityInterest" = %s, "StockholdersEquity" = %s,
        "CapitalStock" = %s, "PreferredStock" = %s, "CommonStock" = %s,
        "AdditionalPaidInCapital" = %s, "RetainedEarnings" = %s, "GainsLossesNotAffectingRetainedEarnings" = %s, 
        "OtherEquityAdjustments" = %s, "MinorityInterest" = %s, "TotalCapitalization" = %s, 
        "CommonStockEquity" = %s, "CapitalLeaseObligations" = %s, "NetTangibleAssets" = %s, 
        "WorkingCapital" = %s, "InvestedCapital" = %s, "TangibleBookValue" = %s, 
        "TotalDebt" = %s, "NetDebt" = %s, "ShareIssued" = %s, 
        "OrdinarySharesNumber" = %s
        WHERE Ticker = %s;
        """
        values = [   
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            row['TotalAssets'],
            row['CurrentAssets'],
            row['CashCashEquivalentsShortTermInvestments'],
            row['CashAndCashEquivalents'],
            row['OtherShortTermInvestments'],
            row['Receivables'],
            row['AccountsReceivable'],
            row['Inventory'],
            row['RawMaterials'],
            row['WorkInProcess'],
            row['FinishedGoods'],
            row['OtherCurrentAssets'],
            row['TotalNonCurrentAssets'],
            row['NetPPE'],
            row['GrossPPE'],
            row['Properties'],
            row['LandAndImprovements'],
            row['MachineryFurnitureEquipment'],
            row['OtherProperties'],
            row['ConstructionInProgress'],
            row['Leases'],
            row['AccumulatedDepreciation'],
            row['GoodwillAndOtherIntangibleAssets'],
            row['Goodwill'],
            row['OtherIntangibleAssets'],
            row['NonCurrentNoteReceivables'],
            row['OtherNonCurrentAssets'],
            row['TotalLiabilitiesNetMinorityInterest'],
            row['CurrentLiabilities'],
            row['PayablesAndAccruedExpenses'],
            row['Payables'],
            row['AccountsPayable'],
            row['TotalTaxPayable'],
            row['CurrentAccruedExpenses'],
            row['InterestPayable'],
            row['CurrentProvisions'],
            row['CurrentDebtAndCapitalLeaseObligation'],
            row['CurrentDebt'],
            row['OtherCurrentBorrowings'],
            row['CurrentCapitalLeaseObligation'],
            row['CurrentDeferredLiabilities'],
            row['CurrentDeferredRevenue'],
            row['OtherCurrentLiabilities'],
            row['TotalNonCurrentLiabilitiesNetMinorityInterest'],
            row['LongTermProvisions'],
            row['LongTermDebtAndCapitalLeaseObligation'],
            row['LongTermDebt'],
            row['LongTermCapitalLeaseObligation'],
            row['NonCurrentDeferredLiabilities'],
            row['NonCurrentDeferredTaxesLiabilities'],
            row['NonCurrentDeferredRevenue'],
            row['NonCurrentAccruedExpenses'],
            row['PreferredSecuritiesOutsideStockEquity'],
            row['OtherNonCurrentLiabilities'],
            row['TotalEquityGrossMinorityInterest'],
            row['StockholdersEquity'],
            row['CapitalStock'],
            row['PreferredStock'],
            row['CommonStock'],
            row['AdditionalPaidInCapital'],
            row['RetainedEarnings'],
            row['GainsLossesNotAffectingRetainedEarnings'],
            row['OtherEquityAdjustments'],
            row['MinorityInterest'],
            row['TotalCapitalization'],
            row['CommonStockEquity'],
            row['CapitalLeaseObligations'],
            row['NetTangibleAssets'],
            row['WorkingCapital'],
            row['InvestedCapital'],
            row['TangibleBookValue'],
            row['TotalDebt'],
            row['NetDebt'],
            row['ShareIssued'],
            row['OrdinarySharesNumber'],
            ticker]
        cur.execute(update_query, values)
    else:
        insert_query = """
        INSERT INTO balancesheet (Ticker, 
        "Ticker.Date", "Ticker.Exchange", StockExchange, 
        Name, "TotalAssets", "CurrentAssets", 
        "CashCashEquivalentsShortTermInvestments", "CashAndCashEquivalents", "OtherShortTermInvestments",
        "Receivables","AccountsReceivable", "Inventory", 
        "RawMaterials", "WorkInProcess", "FinishedGoods", 
        "OtherInventories", "PrepaidAssets", "RestrictedCash", 
        "OtherCurrentAssets", "TotalNonCurrentAssets", "NetPPE", 
        "GrossPPE", "Properties", "LandAndImprovements", 
        "MachineryFurnitureEquipment", "OtherProperties", "ConstructionInProgress",
        "Leases", "AccumulatedDepreciation", "GoodwillAndOtherIntangibleAssets", 
        "Goodwill", "OtherIntangibleAssets", "NonCurrentNoteReceivables", 
        "OtherNonCurrentAssets", "TotalLiabilitiesNetMinorityInterest", "CurrentLiabilities",
        "PayablesAndAccruedExpenses", "Payables", "AccountsPayable", 
        "TotalTaxPayable", "CurrentAccruedExpenses", "InterestPayable", 
        "CurrentProvisions", "CurrentDebtAndCapitalLeaseObligation", "CurrentDebt", 
        "OtherCurrentBorrowings","CurrentCapitalLeaseObligation", "CurrentDeferredLiabilities", 
        "CurrentDeferredRevenue", "OtherCurrentLiabilities", "TotalNonCurrentLiabilitiesNetMinorityInterest", 
        "LongTermProvisions", "LongTermDebtAndCapitalLeaseObligation", "LongTermDebt", 
        "LongTermCapitalLeaseObligation", "NonCurrentDeferredLiabilities", "NonCurrentDeferredTaxesLiabilities",
        "NonCurrentDeferredRevenue", "NonCurrentAccruedExpenses", "PreferredSecuritiesOutsideStockEquity", 
        "OtherNonCurrentLiabilities", "TotalEquityGrossMinorityInterest", "StockholdersEquity", 
        "CapitalStock", "PreferredStock", "CommonStock", 
        "AdditionalPaidInCapital", "RetainedEarnings", "GainsLossesNotAffectingRetainedEarnings", "OtherEquityAdjustments",
        "MinorityInterest", "TotalCapitalization", "CommonStockEquity", "CapitalLeaseObligations", "NetTangibleAssets",
        "WorkingCapital", "InvestedCapital", "TangibleBookValue", "TotalDebt", "NetDebt", "ShareIssued", "OrdinarySharesNumber"
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
        values = [
            ticker,
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            int(row['TotalAssets']) if row['TotalAssets'] is not None else None,
            int(row['CurrentAssets']) if row['CurrentAssets'] is not None else None,
            int(row['CashCashEquivalentsShortTermInvestments']) if row['CashCashEquivalentsShortTermInvestments'] is not None else None,
            int(row['CashAndCashEquivalents']) if row['CashAndCashEquivalents'] is not None else None,
            int(row['OtherShortTermInvestments']) if row['OtherShortTermInvestments'] is not None else None,
            int(row['Receivables']) if row['Receivables'] is not None else None,
            int(row['AccountsReceivable']) if row['AccountsReceivable'] is not None else None,
            int(row['Inventory']) if row['Inventory'] is not None else None,
            int(row['RawMaterials']) if row['RawMaterials'] is not None else None,
            int(row['WorkInProcess']) if row['WorkInProcess'] is not None else None,
            int(row['FinishedGoods']) if row['FinishedGoods'] is not None else None,
            int(row['OtherInventories']) if row['OtherInventories'] is not None else None,
            int(row['PrepaidAssets']) if row['PrepaidAssets'] is not None else None,
            int(row['RestrictedCash']) if row['RestrictedCash'] is not None else None,
            int(row['OtherCurrentAssets']) if row['OtherCurrentAssets'] is not None else None,
            int(row['TotalNonCurrentAssets']) if row['TotalNonCurrentAssets'] is not None else None,
            int(row['NetPPE']) if row['NetPPE'] is not None else None,
            int(row['GrossPPE']) if row['GrossPPE'] is not None else None,
            int(row['Properties']) if row['Properties'] is not None else None,
            int(row['LandAndImprovements']) if row['LandAndImprovements'] is not None else None,
            int(row['MachineryFurnitureEquipment']) if row['MachineryFurnitureEquipment'] is not None else None,
            int(row['OtherProperties']) if row['OtherProperties'] is not None else None,
            int(row['ConstructionInProgress']) if row['ConstructionInProgress'] is not None else None,
            int(row['Leases']) if row['Leases'] is not None else None ,
            int(row['AccumulatedDepreciation']) if row['AccumulatedDepreciation'] is not None else None ,
            int(row['GoodwillAndOtherIntangibleAssets']) if row['GoodwillAndOtherIntangibleAssets'] is not None else None ,
            int(row['Goodwill']) if row['Goodwill'] is not None else None ,
            int(row['OtherIntangibleAssets']) if row['OtherIntangibleAssets'] is not None else None ,
            int(row['NonCurrentNoteReceivables']) if row['NonCurrentNoteReceivables'] is not None else None ,
            int(row['OtherNonCurrentAssets']) if row['OtherNonCurrentAssets'] is not None else None ,
            int(row['TotalLiabilitiesNetMinorityInterest']) if row['TotalLiabilitiesNetMinorityInterest'] is not None else None ,
            int(row['CurrentLiabilities']) if row['CurrentLiabilities'] is not None else None ,
            int(row['PayablesAndAccruedExpenses']) if row['PayablesAndAccruedExpenses'] is not None else None ,
            int(row['Payables']) if row['Payables'] is not None else None ,
            int(row['AccountsPayable']) if row['AccountsPayable'] is not None else None ,
            int(row['TotalTaxPayable']) if row['TotalTaxPayable'] is not None else None ,
            int(row['CurrentAccruedExpenses']) if row['CurrentAccruedExpenses'] is not None else None ,
            int(row['InterestPayable']) if row['InterestPayable'] is not None else None,
            int(row['CurrentProvisions']) if row['CurrentProvisions'] is not None else None ,
            int(row['CurrentDebtAndCapitalLeaseObligation']) if row['CurrentDebtAndCapitalLeaseObligation'] is not None else None ,
            int(row['CurrentDebt']) if row['CurrentDebt'] is not None else None ,
            int(row['OtherCurrentBorrowings']) if row['OtherCurrentBorrowings'] is not None else None ,
            int(row['CurrentCapitalLeaseObligation']) if row['CurrentCapitalLeaseObligation'] is not None else None ,
            int(row['CurrentDeferredLiabilities']) if row['FinishedGoods'] is not None else None ,
            int(row['CurrentDeferredRevenue']) if row['CurrentDeferredLiabilities'] is not None else None ,
            int(row['OtherCurrentLiabilities']) if row['OtherCurrentLiabilities'] is not None else None ,
            int(row['TotalNonCurrentLiabilitiesNetMinorityInterest']) if row['TotalNonCurrentLiabilitiesNetMinorityInterest'] is not None else None ,
            int(row['LongTermProvisions']) if row['LongTermProvisions'] is not None else None ,
            int(row['LongTermDebtAndCapitalLeaseObligation']) if row['LongTermDebtAndCapitalLeaseObligation'] is not None else None ,
            int(row['LongTermDebt']) if row['LongTermDebt'] is not None else None ,
            int(row['LongTermCapitalLeaseObligation']) if row['LongTermCapitalLeaseObligation'] is not None else None ,
            int(row['NonCurrentDeferredLiabilities']) if row['NonCurrentDeferredLiabilities'] is not None else None ,
            int(row['NonCurrentDeferredTaxesLiabilities']) if row['NonCurrentDeferredTaxesLiabilities'] is not None else None ,
            int(row['NonCurrentDeferredRevenue']) if row['NonCurrentDeferredRevenue'] is not None else None ,
            int(row['NonCurrentAccruedExpenses']) if row['NonCurrentAccruedExpenses'] is not None else None ,
            int(row['PreferredSecuritiesOutsideStockEquity']) if row['PreferredSecuritiesOutsideStockEquity'] is not None else None ,
            int(row['OtherNonCurrentLiabilities']) if row['OtherNonCurrentLiabilities'] is not None else None ,
            int(row['TotalEquityGrossMinorityInterest']) if row['TotalEquityGrossMinorityInterest'] is not None else None ,
            int(row['StockholdersEquity']) if row['StockholdersEquity'] is not None else None ,
            int(row['CapitalStock']) if row['CapitalStock'] is not None else None ,
            int(row['PreferredStock']) if row['PreferredStock'] is not None else None ,
            int(row['CommonStock']) if row['CommonStock'] is not None else None,
            int(row['AdditionalPaidInCapital']) if row['AdditionalPaidInCapital'] is not None else None ,
            int(row['RetainedEarnings']) if row['RetainedEarnings'] is not None else None ,
            int(row['GainsLossesNotAffectingRetainedEarnings']) if row['GainsLossesNotAffectingRetainedEarnings'] is not None else None ,
            int(row['OtherEquityAdjustments']) if row['OtherEquityAdjustments'] is not None else None ,
            int(row['MinorityInterest']) if row['MinorityInterest'] is not None else None ,
            int(row['TotalCapitalization']) if row['TotalCapitalization'] is not None else None ,
            int(row['CommonStockEquity']) if row['CommonStockEquity'] is not None else None ,
            int(row['CapitalLeaseObligations']) if row['CapitalLeaseObligations'] is not None else None ,
            int(row['NetTangibleAssets']) if row['NetTangibleAssets'] is not None else None ,
            int(row['WorkingCapital']) if row['WorkingCapital'] is not None else None ,
            int(row['InvestedCapital']) if row['InvestedCapital'] is not None else None ,
            int(row['TangibleBookValue']) if row['TangibleBookValue'] is not None else None ,
            int(row['TotalDebt']) if row['TotalDebt'] is not None else None ,
            int(row['NetDebt']) if row['NetDebt'] is not None else None ,
            int(row['ShareIssued']) if row['ShareIssued'] is not None else None ,
            int(row['OrdinarySharesNumber']) if row['OrdinarySharesNumber'] is not None else None 
        ]

# Execute the query with the adapted values
        cur.execute(insert_query, values)

def update_or_insert_cash_flow_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM cashflow WHERE 'Ticker.Date' = %s;"
    cur.execute(check_query, (ticker,))
    count = cur.fetchone()[0]

    if count > 0:
        update_query = """
        UPDATE cashflow
        SET "Ticker.Date" = %s, "Ticker.Exchange" = %s, "StockExchange" = %s, OperatingCashFlow = %s,
CashFlowFromContinuingOperatingActivities = %s, NetIncomeFromContinuingOperations = %s, OperatingGainsLosses = %s, GainLossOnSaleOfPPE = %s,
NetForeignCurrencyExchangeGainLoss = %s, DepreciationAmortizationDepletion = %s, DepreciationAndAmortization = %s, Depreciation = %s, AssetImpairmentCharge = %s, StockBasedCompensation = %s,
OtherNonCashItems = %s, ChangeInWorkingCapital = %s, ChangeInReceivables = %s, ChangesInAccountReceivables = %s, ChangeInInventory = %s,
ChangeInPrepaidAssets = %s, ChangeInPayablesAndAccruedExpense = %s, ChangeInOtherCurrentAssets = %s, ChangeInOtherCurrentLiabilities = %s, ChangeInOtherWorkingCapital = %s,
InvestingCashFlow = %s, CashFlowFromContinuingInvestingActivities = %s, CapitalExpenditureReported = %s, NetPPEPurchaseAndSale = %s, PurchaseOfPPE = %s, NetIntangiblesPurchaseAndSale = %s,
PurchaseOfIntangibles = %s, SaleOfIntangibles = %s, NetBusinessPurchaseAndSale = %s, PurchaseOfBusiness = %s, SaleOfBusiness = %s, NetInvestmentPurchaseAndSale = %s, PurchaseOfInvestment = %s, SaleOfInvestment = %s,
NetOtherInvestingChanges = %s, FinancingCashFlow = %s, CashFlowFromContinuingFinancingActivities = %s, NetIssuancePaymentsOfDebt = %s, NetLongTermDebtIssuance = %s, LongTermDebtIssuance = %s, LongTermDebtPayments = %s, NetCommonStockIssuance = %s, CommonStockIssuance = %s, ProceedsFromStockOptionExercised = %s,
        NetOtherFinancingCharges = %s, EndCashPosition = %s, ChangesInCash = %s, EffectOfExchangeRateChanges = %s, BeginningCashPosition = %s, IncomeTaxPaidSupplementalData = %s, InterestPaidSupplementalData = %s, 
CapitalExpenditure = %s, IssuanceOfCapitalStock = %s, IssuanceOfDebt = %s, RepaymentOfDebt = %s, FreeCashFlow = %s,
        Name = %s
        WHERE Ticker = %s;
        """
        values = [   
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            row['OperatingCashFlow'],
            row['CashFlowFromContinuingOperatingActivities'],
            row['NetIncomeFromContinuingOperations'],
            row['OperatingGainsLosses'],
            row['GainLossOnSaleOfPPE'],
            row['NetForeignCurrencyExchangeGainLoss'],
            row['DepreciationAmortizationDepletion'],
            row['DepreciationAndAmortization'],
            row['Depreciation'],
            row['AssetImpairmentCharge'],
            row['StockBasedCompensation'],
            row['OtherNonCashItems'],
            row['ChangeInWorkingCapital'],
            row['ChangeInReceivables'],
            row['ChangesInAccountReceivables'],
            row['ChangeInInventory'],
            row['ChangeInPrepaidAssets'],
            row['ChangeInPayablesAndAccruedExpense'],
            row['ChangeInOtherCurrentAssets'],
            row['ChangeInOtherCurrentLiabilities'],
            row['ChangeInOtherWorkingCapital'],
            row['InvestingCashFlow'],
            row['CashFlowFromContinuingInvestingActivities'],
            row['CapitalExpenditureReported'],
            row['NetPPEPurchaseAndSale'],
            row['PurchaseOfPPE'],
            row['NetIntangiblesPurchaseAndSale'],
            row['PurchaseOfIntangibles'],
            row['SaleOfIntangibles'],
            row['NetBusinessPurchaseAndSale'],
            row['PurchaseOfBusiness'],
            row['SaleOfBusiness'],
            row['NetInvestmentPurchaseAndSale'],
            row['PurchaseOfInvestment'],
            row['SaleOfInvestment'],
            row['NetOtherInvestingChanges'],
            row['FinancingCashFlow'],
            row['CashFlowFromContinuingFinancingActivities'],
            row['NetIssuancePaymentsOfDebt'],
            row['NetLongTermDebtIssuance'],
            row['LongTermDebtIssuance'],
            row['LongTermDebtPayments'],
            row['NetCommonStockIssuance'],
            row['CommonStockIssuance'],
            row['ProceedsFromStockOptionExercised'], 
            
            row['NetOtherFinancingCharges'],
            row['EndCashPosition'],
            row['ChangesInCash'],
            row['EffectOfExchangeRateChanges'],
            row['BeginningCashPosition'],
            row['IncomeTaxPaidSupplementalData'],
            row['InterestPaidSupplementalData'],
            row['CapitalExpenditure'],
            row['IssuanceOfCapitalStock'],
            row['IssuanceOfDebt'],
            row['RepaymentOfDebt'],
            row['FreeCashFlow'],
            ticker]
        cur.execute(update_query, values)
    else:
        insert_query = """
        INSERT INTO cashflow (Ticker, 
        "Ticker.Date", "Ticker.Exchange", Name, StockExchange, OperatingCashFlow,
        CashFlowFromContinuingOperatingActivities, NetIncomeFromContinuingOperations, OperatingGainsLosses, GainLossOnSaleOfPPE, NetForeignCurrencyExchangeGainLoss, DepreciationAmortizationDepletion,
DepreciationAndAmortization, Depreciation, AssetImpairmentCharge, StockBasedCompensation, OtherNonCashItems, ChangeInWorkingCapital, ChangeInReceivables, ChangesInAccountReceivables,
ChangeInInventory, ChangeInPrepaidAssets, ChangeInPayablesAndAccruedExpense, ChangeInOtherCurrentAssets, ChangeInOtherCurrentLiabilities, ChangeInOtherWorkingCapital, InvestingCashFlow, CashFlowFromContinuingInvestingActivities, CapitalExpenditureReported, NetPPEPurchaseAndSale,PurchaseOfPPE,
NetIntangiblesPurchaseAndSale, PurchaseOfIntangibles, SaleOfIntangibles, NetBusinessPurchaseAndSale, PurchaseOfBusiness, SaleOfBusiness, NetInvestmentPurchaseAndSale, PurchaseOfInvestment,
SaleOfInvestment, NetOtherInvestingChanges, FinancingCashFlow, CashFlowFromContinuingFinancingActivities,
NetIssuancePaymentsOfDebt, NetLongTermDebtIssuance, LongTermDebtIssuance, LongTermDebtPayments, NetCommonStockIssuance, CommonStockIssuance, ProceedsFromStockOptionExercised,
NetOtherFinancingCharges, EndCashPosition, ChangesInCash, EffectOfExchangeRateChanges,
BeginningCashPosition, IncomeTaxPaidSupplementalData, InterestPaidSupplementalData, CapitalExpenditure,
IssuanceOfCapitalStock, IssuanceOfDebt, RepaymentOfDebt, FreeCashFlow

        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
        values = [
            ticker,
            row['Ticker.Date'],
            row['Ticker.Exchange'],
            row['StockExchange'],
            row['Name'],
            row['OperatingCashFlow'],
            row['CashFlowFromContinuingOperatingActivities'],
            row['NetIncomeFromContinuingOperations'],
            row['OperatingGainsLosses'],
            row['GainLossOnSaleOfPPE'],
            row['NetForeignCurrencyExchangeGainLoss'],
            row['DepreciationAmortizationDepletion'],
            row['DepreciationAndAmortization'],
            row['Depreciation'],
            row['AssetImpairmentCharge'],
            row['StockBasedCompensation'],
            row['OtherNonCashItems'],
            row['ChangeInWorkingCapital'],
            row['ChangeInReceivables'],
            row['ChangesInAccountReceivables'],
            row['ChangeInInventory'],
            row['ChangeInPrepaidAssets'],
            row['ChangeInPayablesAndAccruedExpense'],
            row['ChangeInOtherCurrentAssets'],
            row['ChangeInOtherCurrentLiabilities'],
            row['ChangeInOtherWorkingCapital'],
            row['InvestingCashFlow'],
            row['CashFlowFromContinuingInvestingActivities'],
            row['CapitalExpenditureReported'],
            row['NetPPEPurchaseAndSale'],
            row['PurchaseOfPPE'],
            row['NetIntangiblesPurchaseAndSale'],
            row['PurchaseOfIntangibles'],
            row['SaleOfIntangibles'],
            row['NetBusinessPurchaseAndSale'],
            row['PurchaseOfBusiness'],
            row['SaleOfBusiness'],
            row['NetInvestmentPurchaseAndSale'],
            row['PurchaseOfInvestment'],
            row['SaleOfInvestment'],
            row['NetOtherInvestingChanges'],
            row['FinancingCashFlow'],
            row['CashFlowFromContinuingFinancingActivities'],
            row['NetIssuancePaymentsOfDebt'],
            row['NetLongTermDebtIssuance'],
            row['LongTermDebtIssuance'],
            row['LongTermDebtPayments'],
            row['NetCommonStockIssuance'],
            row['CommonStockIssuance'],
            row['ProceedsFromStockOptionExercised'],
            row['NetOtherFinancingCharges'],
            row['EndCashPosition'],
            row['ChangesInCash'],
            row['EffectOfExchangeRateChanges'],
            row['BeginningCashPosition'],
            row['IncomeTaxPaidSupplementalData'],
            row['InterestPaidSupplementalData'],
            row['CapitalExpenditure'],
            row['IssuanceOfCapitalStock'],
            row['IssuanceOfDebt'],
            row['RepaymentOfDebt'],
            row['FreeCashFlow']
        ]

    # Execute the query with the adapted values
        cur.execute(insert_query, values)
        
        
# yfinance new data
# variables not used: "Total Unusual Items", "Total Unusual Items Excluding Goodwill","Gain On Sale Of Security", "Selling And Marketing Expense", General And Administrative Expense'
def create_yf_income_statement_table_if_not_exists(cur):
    create_yf_income_statement_table_query = """
    CREATE TABLE IF NOT EXISTS yfIncomeStatement (
        Ticker VARCHAR(50),
        Date DATE, 
        "Tax Effect Of Unusual Items" TEXT,
        "Tax Rate For Calcs" TEXT,
        "Normalized EBITDA" TEXT,
        "Net Income From Continuing Operation Net Minority Interest" TEXT,
        "Reconciled Cost Of Revenue" TEXT,
        "EBITDA" TEXT,
        "EBIT" TEXT,
        "Net Interest Income" TEXT,
        "Interest Expense" TEXT,
        "Interest Income" TEXT,
        "Normalized Income" TEXT,
        "Net Income From Continuing And Discontinued Operation" TEXT,
        "Total Expenses" TEXT,
        "Total Operating Income As Reported" TEXT,
        "Diluted Average Shares" TEXT,
        "Basic Average Shares" TEXT,
        "Diluted EPS" TEXT,
        "Basic EPS" TEXT,
        "Diluted NI Availto Com Stockholders" TEXT,
        "Net Income Common Stockholders" TEXT,
        "Net Income" TEXT,
        "Net Income Including Noncontrolling Interests" TEXT,
        "Net Income Continuous Operations" TEXT,
        "Tax Provision" TEXT,
        "Pretax Income" TEXT,
        "Other Income Expense" TEXT,
        "Other Non Operating Income Expenses" TEXT,

        "Net Non Operating Interest Income Expense" TEXT,
        "Interest Expense Non Operating" TEXT,
        "Interest Income Non Operating" TEXT,
        "Operating Income" TEXT,
        "Operating Expense" TEXT,
        "Selling General And Administration" TEXT,
        "Gross Profit" TEXT,
        "Cost Of Revenue" TEXT,
        "Total Revenue" TEXT,
        "Operating Revenue" TEXT,
        PRIMARY KEY (Ticker, Date)
    );
    """
    cur.execute(create_yf_income_statement_table_query)

# variables not used: Capital Lease Obligations, Total Non-Current Liabilities Net Minority Interest, Other Non-Current Liabilities, Trade and Other Payables Non-Current, Non-Current Deferred Liabilities, Non-Current Deferred Revenue, Non-Current Deferred Taxes Liabilities, Long-Term Debt And Capital Lease Obligation, Long-Term Capital Lease Obligation, Long-Term Debt, Other Current Liabilities, Pension and Other Post Retirement Benefit Plans Current, Total Tax Payable, Income Tax Payable,Total Non-Current Assets,Other Non-Current Assets, Long-Term Equity Investment, Net PPE (Net Property, Plant, and Equipment), Gross PPE (Gross Property, Plant, and Equipment), Leases, Other Properties, Hedging Assets Current, Work In Process, Allowance For Doubtful Accounts Receivable, Gross Accounts Receivable,Cash Equivalents, Cash Financial
def create_yf_balance_sheet_table_if_not_exists(cur):
    create_yf_balance_sheet_table_query = """
    CREATE TABLE IF NOT EXISTS yfBalanceSheet(
        Ticker VARCHAR(50),
        Date DATE, 
        "Ordinary Shares Number" TEXT,
        "Share Issued" TEXT,
        "Net Debt" TEXT, 
        "Total Debt" TEXT,
        "Tangible Book Value" TEXT,
        "Invested Capital" TEXT,
        "Working Capital" TEXT,
        "Net Tangible Assets" TEXT,
        "Common Stock Equity" TEXT,
        "Total Capitalization" TEXT,
        "Total Equity Gross Minority Interest" TEXT,
        "Stockholders Equity" TEXT,
        "Gains Losses Not Affecting Retained Earnings" TEXT,
        "Other Equity Adjustments" TEXT,
        "Retained Earnings" TEXT,
        "Capital Stock" TEXT,
        "Common Stock" TEXT,
        "Total Liabilities Net Minority Interest" TEXT,
        "Current Liabilities" TEXT,
        PRIMARY KEY (Ticker, Date)
    );
    """
    cur.execute(create_yf_balance_sheet_table_query)
  
# variables not used: Common Stock Issuance, Cash Dividends Paid, Common Stock Payments, Net Short Term Debt Issuance, Short Term Debt Issuance, Change In Other Current Liabilities, Cash Flow From Continuing Operating Activities, Change In Working Capital, Change In Other Working Capital, Change In Payables And Accrued Expense, Change In Payable, Change In Account Payable, Change In Inventory, Depreciation, Change In Receivables, Changes In Account Receivables, Stock Based Compensation, Deferred Tax, Deferred Income Tax, Depreciation Amortization Depletion, Depreciation And Amortization, Operating Gains Losses'
def create_yf_cash_flow_table_if_not_exists(cur):
    create_yf_cash_flow_table_query = """
    CREATE TABLE IF NOT EXISTS yfCashFlow (
        Ticker VARCHAR(50),
        Date DATE, 
        "Free Cash Flow" TEXT,
        "Repayment Of Debt" TEXT,
        "Issuance Of Debt" TEXT,
        "Capital Expenditure" TEXT,
        "End Cash Position" TEXT,
        "Beginning Cash Position" TEXT,
        "Changes In Cash" TEXT,
        "Financing Cash Flow" TEXT,
        "Cash Flow From Continuing Financing Activities" TEXT,
        "Net Other Financing Charges" TEXT,
        "Net Common Stock Issuance" TEXT,
        "Net Issuance Payments Of Debt" TEXT,
        "Net Long Term Debt Issuance" TEXT,
        "Long Term Debt Payments" TEXT,
        "Long Term Debt Issuance" TEXT,
        "Investing Cash Flow" TEXT,
        "Cash Flow From Continuing Investing Activities" TEXT,
        "Net Other Investing Changes" TEXT,
        "Net Investment Purchase And Sale" TEXT,
        "Sale Of Investment" TEXT,
        "Purchase Of Investment" TEXT,
        "Net Business Purchase And Sale" TEXT,
        "Purchase Of Business" TEXT,
        "Net PPE Purchase And Sale" TEXT,
        "Purchase Of PPE" TEXT,
        "Operating Cash Flow" TEXT,
        PRIMARY KEY (Ticker, Date)

    );
    """
    cur.execute(create_yf_cash_flow_table_query)
    
 #DONE      

def update_or_insert_yf_income_statement_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfIncomeStatement WHERE Date = %s::date AND Ticker = %s::text;"
    cur.execute(check_query, (row['Date'], ticker))
    print(row)
    count = cur.fetchone()[0]
    print(count)
    if count == 0:
        insert_query = """
        INSERT INTO yfIncomeStatement (Ticker, Date, "Tax Effect Of Unusual Items",
"Tax Rate For Calcs", "Normalized EBITDA",
"Net Income From Continuing Operation Net Minority Interest",
"Reconciled Cost Of Revenue",
"EBITDA",
"EBIT",
"Net Interest Income",
"Interest Expense",
"Interest Income",
"Normalized Income",
"Net Income From Continuing And Discontinued Operation",
"Total Expenses",
"Total Operating Income As Reported",
"Diluted Average Shares",
"Basic Average Shares",
"Diluted EPS",
"Basic EPS",
"Diluted NI Availto Com Stockholders",
"Net Income Common Stockholders",
"Net Income",
"Net Income Including Noncontrolling Interests",
"Net Income Continuous Operations",
"Tax Provision",
"Pretax Income",
"Other Income Expense",
"Other Non Operating Income Expenses",

"Net Non Operating Interest Income Expense",
"Interest Expense Non Operating",
"Interest Income Non Operating",
"Operating Income",
"Operating Expense",
"Selling General And Administration",



"Gross Profit",
"Cost Of Revenue",
"Total Revenue",
"Operating Revenue")
VALUES  
        ( %s, %s, %s, %s, %s,%s,%s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)        
        ON CONFLICT (Ticker, Date) DO NOTHING;

        """
        cur.execute(insert_query, (
           ticker,
            row['Date'],
            row['Tax Effect Of Unusual Items'],
            row['Tax Rate For Calcs'],
            row['Normalized EBITDA'],
            row['Net Income From Continuing Operation Net Minority Interest'],
            row['Reconciled Cost Of Revenue'],
            row['EBITDA'],
            row['EBIT'],
            row['Net Interest Income'],
            row['Interest Expense'],
            row['Interest Income'],
            row['Normalized Income'],
            row['Net Income From Continuing And Discontinued Operation'],
            row['Total Expenses'],
            row['Total Operating Income As Reported'],
            row['Diluted Average Shares'],
            row['Basic Average Shares'],
            row['Diluted EPS'],
            row['Basic EPS'],
            row['Diluted NI Availto Com Stockholders'],
            row['Net Income Common Stockholders'],
            row['Net Income'],
            row['Net Income Including Noncontrolling Interests'],
            row['Net Income Continuous Operations'],
            row['Tax Provision'],
            row['Pretax Income'],
            row['Other Income Expense'],
            row['Other Non Operating Income Expenses'],

            row['Net Non Operating Interest Income Expense'],
            row['Interest Expense Non Operating'],
            row['Interest Income Non Operating'],
            row['Operating Income'],
            row['Operating Expense'],
            row['Selling General And Administration'],
            row['Gross Profit'],
            row['Cost Of Revenue'],
            row['Total Revenue'],
            row['Operating Revenue']
        ))

def update_or_insert_yf_balance_sheet_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfBalanceSheet WHERE Date = %s::date AND Ticker = %s::text;"
    cur.execute(check_query, (row['Date'], ticker))
    print(row)
    count = cur.fetchone()[0]
    print(count)
    if count == 0:
        insert_query = """
        INSERT INTO yfBalanceSheet (Ticker, Date, "Ordinary Shares Number", "Share Issued",
"Net Debt", "Total Debt", "Tangible Book Value", "Invested Capital", "Working Capital",
"Net Tangible Assets", "Common Stock Equity", "Total Capitalization", "Total Equity Gross Minority Interest", "Stockholders Equity",
"Gains Losses Not Affecting Retained Earnings","Other Equity Adjustments", "Retained Earnings", "Capital Stock",
"Common Stock", "Total Liabilities Net Minority Interest","Current Liabilities")
VALUES  
        (%s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)        ON CONFLICT (Ticker, Date) DO NOTHING;

        """
        # , %s, %s, %s, %s, %s
        cur.execute(insert_query, (
           ticker,
            row['Date'],
            row['Ordinary Shares Number'],
            row['Share Issued'],
            row['Net Debt'],
            row['Total Debt'],
            row['Tangible Book Value'],
            row['Invested Capital'],
            row['Working Capital'],
            row['Net Tangible Assets'],
            row['Common Stock Equity'],
            row['Total Capitalization'],
            row['Total Equity Gross Minority Interest'],
            row['Stockholders Equity'],
            row['Gains Losses Not Affecting Retained Earnings'],
            row['Other Equity Adjustments'],
            row['Retained Earnings'],
            row['Capital Stock'],
            row['Common Stock'],
            row['Total Liabilities Net Minority Interest'],
            row['Current Liabilities']
            ))

def update_or_insert_yf_cash_flow_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfCashFlow WHERE Date = %s AND Ticker = %s;"
    cur.execute(check_query, (row['Date'], ticker))
    count = cur.fetchone()[0]

    if count > 0:
        pass;
    else:
        insert_query = """
        INSERT INTO yfCashFlow (Ticker, Date, "Free Cash Flow",  "Repayment Of Debt", "Issuance Of Debt",
"Capital Expenditure", "End Cash Position", "Beginning Cash Position", "Changes In Cash", "Financing Cash Flow", "Cash Flow From Continuing Financing Activities", 
"Net Other Financing Charges","Net Common Stock Issuance",         "Net Issuance Payments Of Debt","Net Long Term Debt Issuance",        "Long Term Debt Payments", "Long Term Debt Issuance",
"Investing Cash Flow", "Cash Flow From Continuing Investing Activities", "Net Other Investing Changes",
"Net Investment Purchase And Sale", "Sale Of Investment", "Purchase Of Investment", "Net Business Purchase And Sale",
"Purchase Of Business", "Net PPE Purchase And Sale", "Purchase Of PPE", "Operating Cash Flow")        VALUES 
        (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Ticker, Date) DO NOTHING;
;
        """
        cur.execute(insert_query, (
            ticker,
            row['Date'], 
            row['Free Cash Flow'],
            row['Repayment Of Debt'],
            row['Issuance Of Debt'],
            row['Capital Expenditure'],
            row['End Cash Position'],
            row['Beginning Cash Position'],
            row['Changes In Cash'],
            row['Financing Cash Flow'],
            row['Cash Flow From Continuing Financing Activities'],
            row['Net Other Financing Charges'],
            row['Net Common Stock Issuance'],
            row['Net Issuance Payments Of Debt'],
            row['Net Long Term Debt Issuance'],
            row['Long Term Debt Payments'],
            row['Long Term Debt Issuance'],
            row['Investing Cash Flow'],
            row['Cash Flow From Continuing Investing Activities'],
            row['Net Other Investing Changes'],
            row['Net Investment Purchase And Sale'],
            row['Sale Of Investment'],
            row['Purchase Of Investment'],
            row['Net Business Purchase And Sale'],
            row['Purchase Of Business'],
            row['Net PPE Purchase And Sale'],
            row['Purchase Of PPE'],
            row['Operating Cash Flow']
            ))

# Daily Data
def create_daily_price_data(cur):
    create_yf_daily_table_query = """
    CREATE TABLE IF NOT EXISTS yfDailyPrice (
        Ticker TEXT,
        Date DATE,
        Open REAL,
        High REAL,
        Low REAL,
        Close REAL,
        Volume INTEGER,
        Dividends REAL, 
        "Stock Splits" REAL,
        PRIMARY KEY (Ticker, Date)
    );
    """
    cur.execute(create_yf_daily_table_query)
    
def update_or_insert_yf_daily_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfDailyPrice WHERE Date = %s::date AND Ticker = %s::text;"
    #print(row)
    #formatted_dates = row.index.strftime('%Y-%m-%d')
    cur.execute(check_query, (row["Date"], ticker))
    count = cur.fetchone()[0]

    if count > 0:
        pass;
    else:
        insert_query = """
        INSERT INTO yfDailyPrice (Ticker, Date, Open, Close, High, Low, Volume, Dividends, "Stock Splits")        VALUES 
        (%s, %s,%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Ticker, Date) DO NOTHING;
        """
        cur.execute(insert_query, (
            ticker,
            row['Date'], 
            row['Open'], 
            row['Close'], 
            row['High'], 
            row['Low'], 
            row['Volume'], 
            row['Dividends'], 
            row["Stock Splits"]
            ))

def get_latest_daily_date(cur, ticker_symbol):
    # Query the database to get the latest date for the given ticker
    query = f"SELECT MAX(Date) FROM yfDailyPrice WHERE Ticker = '{ticker_symbol}';"
    cur.execute(query)
    latest_date = cur.fetchone()[0]
    return latest_date

def set_daily_date_ranges(cur, ticker_symbol):
    latest_date = get_latest_daily_date(cur, ticker_symbol)
    
    if latest_date:
        # Convert the date to a string and then parse it as a datetime
        latest_date = datetime.strptime(str(latest_date), '%Y-%m-%d') + timedelta(days=1)
    else:
        # If there's no data, start from a specific date
        latest_date = datetime.strptime("2000-01-01", '%Y-%m-%d')
    
    # Set the end date to the current date
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    return latest_date.strftime('%Y-%m-%d'), end_date

# intraday table
def create_intraday_price_data(cur):
    create_yf_intraday_table_query = """
    CREATE TABLE IF NOT EXISTS yfIntradayPrice (
        Ticker TEXT,
        Date TIMESTAMP,
        Open REAL,
        Close REAL,
        Delta REAL,
        Volume INTEGER,
        Hourly_RoC REAL, 
        PRIMARY KEY (Ticker, Date)
    );
    """
    cur.execute(create_yf_intraday_table_query)
# insert into intraday
def update_or_insert_yf_intraday_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfIntradayPrice WHERE Date = %s::date AND Ticker = %s::text;"
    print(row)
    #formatted_dates = row.index.strftime('%Y-%m-%d')
    cur.execute(check_query, (row["Date"], ticker))
    count = cur.fetchone()[0]

    if count > 0:
        pass;
    else:
        insert_query = """
        INSERT INTO yfIntradayPrice (Ticker, Date, Open, Close, Volume, Delta, Hourly_RoC)        VALUES 
        (%s, %s,%s, %s, %s, %s, %s) ON CONFLICT (Ticker, Date) DO NOTHING;
        """
        cur.execute(insert_query, (
            ticker,
            row['Date'], 
            row['Open'], 
            row['Close'],
            row['Volume'], 
            row['Delta'], 
            row['Hourly_RoC']
            ))

def get_latest_intraday_datetime(cur, ticker_symbol):
    # Query the database to get the latest datetime for the given ticker
    query = f"SELECT MAX(Date) FROM yfIntradayPrice WHERE Ticker = '{ticker_symbol}';"
    cur.execute(query)
    latest_datetime = cur.fetchone()[0]
    return latest_datetime

def set_intraday_datetime_ranges(cur, ticker_symbol):
    latest_datetime = get_latest_intraday_datetime(cur, ticker_symbol)
    
    if latest_datetime:
        # Increment the latest datetime by one minute
        latest_datetime = latest_datetime + timedelta(minutes=1)
        if latest_datetime.time() == datetime.min.time():
            # If the latest datetime was the last minute of the day, go to the next day
            latest_datetime = latest_datetime.replace(hour=0, minute=0, second=0) + timedelta(days=1)
    else:
        # If there's no data, start from a specific datetime
        latest_datetime = datetime.strptime("2000-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    
    # Set the end datetime to the current datetime
    end_datetime = datetime.now()
    
    return latest_datetime, end_datetime

# general information
def create_general_stock_data(cur):
    create_yf_general_table_query = """
    CREATE TABLE IF NOT EXISTS yfGeneral (
        Ticker TEXT, 
        StockExchange TEXT, 
        Name TEXT, 
        Industry TEXT,  
        Sector TEXT, 
        LongDescription TEXT,
        Website TEXT,
        HeadQuarter TEXT, 
        Founded TEXT,
        PRIMARY KEY (Ticker)
);
    """
    cur.execute(create_yf_general_table_query)            
              
def update_or_insert_yf_general_data(cur, ticker, row):
    check_query = "SELECT COUNT(*) FROM yfGeneral WHERE Ticker = %s::text;"
    cur.execute(check_query, (ticker))
    count = cur.fetchone()[0]
    if count > 0:
        pass;
    else:
        insert_query = """
        INSERT INTO yfIntradayPrice (Ticker, StockExchange, Name, Industry, Sector, LongDescription, Website, HeadQuarter, Founded)        VALUES 
        (%s, %s,%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Ticker) DO NOTHING;
        """
        cur.execute(insert_query, (
            ticker,
            row['StockExchange'], 
            row['Name'], 
            row['Industry'],
            row['Sector'], 
            row['LongDescription'], 
            row['Website'],
            row['HeadQuarter'], 
            row['Founded']
            ))