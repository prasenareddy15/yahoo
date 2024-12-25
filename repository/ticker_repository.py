from ..services.db_service import get_db_engine
def get_ticker_by_id(ticker_id):
    engine = get_db_engine()

    query = """
    SELECT [ticker_id], [symbol], [name], [last_updated], [created_at]
    FROM [yahoo].[dbo].[Ticker_t]
    WHERE [ticker_id] = :ticker_id
    """
    with engine.connect() as connection:
        result = connection.execute(text(query), {"ticker_id": ticker_id})
        ticker = [dict(row) for row in result]
    return ticker
