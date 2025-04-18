import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib

# Load environment variables
k=load_dotenv(dotenv_path=r"C:\Users\mavul\Documents\known\yahoo\env.env")
print(k,'april3')
# Get database connection details from .env
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
print(server,database,username,password)
# URL encoding for the password
password = urllib.parse.quote_plus(password)

# SQLAlchemy connection string
driver = 'ODBC+Driver+17+for+SQL+Server'
connection_string = f'mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes'

# Create database engine
engine = create_engine(connection_string)

def get_db_engine():
    return engine
