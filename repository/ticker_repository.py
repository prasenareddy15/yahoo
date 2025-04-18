from services.db_service import get_db_engine
from sqlalchemy import text
print("apple")
def get_ticker_by_id(ticker_id):
    engine = get_db_engine()

    query = """
    SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
    FROM [yahoo].[dbo].[stock_daily]
    WHERE [ticker_id] = :ticker_id
    """
    with engine.connect() as connection:
        result = connection.execute(text(query), {"ticker_id": ticker_id})
        ticker = [dict(row._mapping) for row in result]
    return ticker
