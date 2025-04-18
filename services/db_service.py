import os
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv
import urllib

# Load environment variables
load_dotenv(dotenv_path=r"C:\Users\mavul\Documents\known\yahoo\env.env")

# Get database connection details from .env
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

print(server, database, username, password)

# URL encode password if needed
password = urllib.parse.quote_plus(password)

# Async connection string using aioodbc
driver = 'ODBC+Driver+17+for+SQL+Server'
connection_string = (
    f"mssql+aioodbc://@{server}/{database}"
    f"?driver={driver}&trusted_connection=yes"
)

# Create async engine
engine = create_async_engine(connection_string, echo=True, future=True)

def get_db_engine():
    print("*******************************engine type", type(engine))
    return engine

# Run async query
async def test_query():
    query = """
        SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
        FROM [yahoo].[dbo].[stock_daily]
        WHERE [ticker_id] = 14
    """
    async with engine.connect() as connection:
        result = await connection.execute(text(query))
        ticker = [dict(row._mapping) for row in result]
        print("dbworking", ticker)

# Entry point
if __name__ == "__main__":
    asyncio.run(test_query())
