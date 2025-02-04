from sqlalchemy import text
import decimal
from services.db_service import get_db_engine

def transform_and_insert_data(source_table, destination_table, stringified_columns):
    engine = get_db_engine()
    select_query = text(f"SELECT * FROM {source_table}")
    with engine.connect() as connection:
        result = connection.execute(select_query)
        rows = result.fetchall()
        column_names = result.keys()
    transformed_data = []
    for row in rows:
        row_dict = dict(zip(column_names, row))
        transformed_row = {}
        for col, value in row_dict.items():
            if col in stringified_columns:
                extracted_values = value.strip('{}').split(',')
                for i in range(stringified_columns[col]):
                    col_name = f"{col}_{i+1}"
                    extracted_value = extracted_values[i].strip('"') if i < len(extracted_values) else None
                    if not extracted_value:  # If the value is empty ("" or None)
                        transformed_row[col_name] = "NULL"  # Set "NULL" explicitly
                    else:
                        transformed_row[col_name] = extracted_value
                    #print(col_name,transformed_row[col_name])
            else:
                transformed_row[col] = value 
        print(transformed_row)
        transformed_data.append(transformed_row)
    if transformed_data:
        columns = transformed_data[0].keys()
        insert_query = f"INSERT INTO {destination_table} ({', '.join(columns)}) VALUES ({', '.join([':' + col for col in columns])})"
        
        with engine.connect() as connection:
            connection.execute(text(insert_query), transformed_data)
            connection.commit()
            print(f"Data successfully inserted into {destination_table}")
def create_destination_table(source_table, destination_table, stringified_columns):
    engine = get_db_engine()
    query_columns = text(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{source_table}'
    """)
    max_length_query = lambda col: f"SELECT MAX(LEN({col})) AS max_length FROM {source_table}"
    column_max_lengths = {}
    
    with engine.connect() as connection:
        columns = connection.execute(query_columns).fetchall()
        for column in columns:
            if column.DATA_TYPE in ['varchar', 'nvarchar', 'char', 'nchar']:
                max_length = connection.execute(text(max_length_query(column.COLUMN_NAME))).scalar()
                column_max_lengths[column.COLUMN_NAME] = max_length or 1 
    drop_query = text(f"DROP TABLE IF EXISTS {destination_table}")
    with engine.connect() as connection:
        connection.execute(drop_query)
        connection.commit()
    create_table_sql = f"CREATE TABLE {destination_table} ("
    for column in columns:
        column_name = column.COLUMN_NAME
        data_type = column.DATA_TYPE

        if column_name in stringified_columns:
            for i in range(1, stringified_columns[column_name] + 1):
                create_table_sql += f"{column_name}_{i} {data_type}("
                create_table_sql += f"{column_max_lengths.get(column_name, 50)})," if data_type in ['varchar', 'nvarchar'] else f"{data_type}), "
        else:
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                max_length = column_max_lengths.get(column_name, 50)
                create_table_sql += f"{column_name} {data_type}({max_length}), "
            else:
                create_table_sql += f"{column_name} {data_type}, "
    create_table_sql = create_table_sql.rstrip(', ') + ')'

    with engine.connect() as connection:
        connection.execute(text(create_table_sql))
        connection.commit()
        print(f"Table {destination_table} created successfully.")
    transform_and_insert_data(source_table, destination_table, stringified_columns)
# Example Usage
if __name__ == "__main__":
    source_table = "Chg_array"
    destination_table = source_table+"_extracted"
    stringified_columns = {
        "chgcd_gl": 5,
        "chgcd_date": 3,
    }
    create_destination_table(source_table, destination_table, stringified_columns)

    
