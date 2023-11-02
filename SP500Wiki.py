import psycopg2
import pandas as pd
import yfinance as yf
from pulldata import *
from datetime import datetime
# Suppress pandas warnings
pd.options.mode.chained_assignment = None

# Get the list of S&P 500 tickers
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
table = pd.read_html(url)[0]

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname='MyDatabase',
    user='postgres',
    password='bobwashere',
    host='localhost',
    port=5432
)

# Create a cursor to execute SQL queries
cur = conn.cursor()

# Create the "keyfacts" table if it doesn't exist
create_SP500Wiki_table_query = """
CREATE TABLE IF NOT EXISTS SP500Wiki (
    Symbol VARCHAR(10) PRIMARY KEY,
    Security TEXT,
    "GICS Sector" TEXT,
    "GICS Sub-Industry" TEXT,
    "Headquarters Location" TEXT,
    "Date added" TEXT,
    CIK NUMERIC(10, 2),
    Founded TEXT
);
"""
cur.execute(create_SP500Wiki_table_query)

# Load data from the DataFrame into the "keyfacts" table, updating if records with the same Ticker already exist
for index, row in table.iterrows():
    symbol = row['Symbol']
    # Check if the record with the same Ticker already exists
    check_query = "SELECT COUNT(*) FROM SP500Wiki WHERE Symbol = %s;"
    cur.execute(check_query, (symbol,))
    count = cur.fetchone()[0]

    if count > 0:
        # Update the existing record
        
        # unique id called "date.ticker" for each table
        
        update_query = """
        UPDATE SP500Wiki
        SET Security= %s, "GICS Sector" = %s, "GICS Sub-Industry" = %s, "Headquarters Location"= %s,  "Date added" = %s, CIK = %s, Founded= %s
        WHERE Symbol = %s;
        """
        cur.execute(update_query, ( 
            row['Security'],
            row['GICS Sector'],
            row['GICS Sub-Industry'],
            row['Headquarters Location'],
            row['Date added'],
            row['CIK'],
            row['Founded'],
            symbol
        ))
    else:
        # Insert a new record
        insert_query = """
        INSERT INTO SP500Wiki(Symbol, Security, "GICS Sector", "GICS Sub-Industry", "Headquarters Location", "Date added", CIK, Founded)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(insert_query, (
            symbol,
            row['Security'],
            row['GICS Sector'],
            row['GICS Sub-Industry'],
            row['Headquarters Location'],
            row['Date added'],
            row['CIK'],
            row['Founded']
        ))

# Commit the changes to the database
conn.commit()

# Close the cursor and the connection
cur.close()
conn.close()