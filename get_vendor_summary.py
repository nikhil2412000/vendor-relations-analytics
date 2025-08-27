import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def create_vendor_summary(conn):
    """Merge tables to get overall vendor summary"""
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p1.VendorNumber,
            p1.VendorName,
            p1.Brand,
            p1.Description,
            p1.PurchasePrice,
            p2.Volume,
            p2.Price AS ActualPrice,
            SUM(p1.Quantity) AS TotalPurchaseQuantity,
            SUM(p1.Dollars) AS TotalPurchaseDollars
        FROM purchases p1 
        JOIN purchase_prices p2 
            ON p1.Brand = p2.Brand 
        WHERE p1.PurchasePrice > 0                                    
        GROUP BY p1.VendorNumber, p1.VendorName, p1.Brand, p1.Description, p1.PurchasePrice, p2.Volume, p2.Price
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars)  AS TotalSalesDollars,
            SUM(SalesPrice)    AS TotalSalesPrice, 
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax)     AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn) 
    
    return vendor_sales_summary 


def clean_data(df):
    """Clean the vendor summary dataframe"""
    # Ensure correct data types
    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].astype('float64')

    # Fill missing values with 0
    df.fillna(0, inplace=True)

    # Strip spaces from categorical columns
    if 'VendorName' in df.columns:
        df['VendorName'] = df['VendorName'].str.strip()
    if 'Description' in df.columns:
        df['Description'] = df['Description'].str.strip()

    # Create new calculated columns
    if {'TotalSalesDollars', 'TotalPurchaseDollars'}.issubset(df.columns):
        df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
        df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'].replace(0, pd.NA)) * 100
        df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, pd.NA)

    if {'TotalSalesQuantity', 'TotalPurchaseQuantity'}.issubset(df.columns):
        df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, pd.NA)

    return df 


if __name__ == '__main__':
    # Create database connection 
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info(f"Preview of summary: \n{summary_df.head()}")

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(f"Preview of cleaned data: \n{clean_df.head()}")

    logging.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed')
