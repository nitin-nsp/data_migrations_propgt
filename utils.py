import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

DB_PARAMS = {
    'host': 'localhost',
    'port': '5011',
    'user': 'postgres',
    'password': 'mysecretpassword'
}

def table(data):
    print(tabulate(data))

def connect_to_db(db_name):
    """Connect to a specific database."""
    conn_params = DB_PARAMS.copy()
    conn_params['dbname'] = db_name
    return psycopg2.connect(**conn_params)

def extract_data(src_conn, table_name):
    """
    Extract data from source database.
    Returns a list of dictionaries, each containing column names, values, and types.
    """
    with src_conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Fetch data
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
        rows = cur.fetchall()

        # Fetch column information
        cur.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = {}
            ORDER BY ordinal_position
        """).format(sql.Literal(table_name)))
        columns_info = cur.fetchall()

        # Create a list of dictionaries with column info and values
        data = []
        for row in rows:
            row_dict = {}
            for col_info in columns_info:
                col_name = col_info['column_name']
                col_type = col_info['data_type']
                row_dict[col_name] =row[col_name]
            # print(row_dict)
            data.append(row_dict)

        return data


def get_table_data(table_name,src_db_name="src_progpt_db"):
     with connect_to_db(db_name=src_db_name) as src_conn:
            
            data = extract_data(src_conn, table_name=table_name)
            return data

def get_query_result(query,db_name,params=None):
    try:
        with connect_to_db(db_name=db_name) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                result = cur.fetchall()
                # print("queryresutlt", result)
                return result
    except Exception as e:
        print (f"error (get query) {str(e)}")
    
        
def load_data(tar_conn, table_name, data):
    """Load data into target database."""
    try:
        # tar_conn=connect_to_db("tar_progpt_db")
        successful_trans,failed_trans=0,0
        columns = list(data[0].keys())
        with tar_conn.cursor() as cur:
            for data_row in data:
                try:
                    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        sql.SQL(', ').join(sql.Placeholder() * len(columns))
                    )
                    rows_to_insert = [data_row[col] for col in data_row]
                    cur.execute(insert_query, rows_to_insert)
                    print("inserted: ")
                    # print(data_row)
                    # input("enter the ===> ")
                    tar_conn.commit()
                    successful_trans+=1
                except Exception as e:
                    # print(f"error(load sql): {str(e)} ")
                    import traceback
                    print(f"error trans: {str(e)}\n{traceback.format_exc()}\n {data_row}")
        
                    # print(data_row)
                    # Roll back the transaction to avoid d
                    # ata inconsistencies
                    tar_conn.rollback()
                    failed_trans+=1
                    continue  # Continue with the next row

            # Commit the transaction if no exceptions occurred
            tar_conn.commit()
            print(f"Successfully loaded {len(data)} rows into {table_name}")
            print(f"success: {successful_trans}\nfailed: {failed_trans}")
    except Exception as e:
        import traceback
        print(f"error load: {str(e)}\n{traceback.format_exc()}")
        
        

def load_data_into_table(tar_conn, table_name, data):
    columns = list(data[0].keys())
    successful_inserts = 0
    failed_inserts = 0

    with tar_conn.cursor() as cur:
        for data_row in data:
            try:
                # Prepare the SQL INSERT query
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                # Extract the values to insert
                rows_to_insert = [data_row[col] for col in columns]
                
                # Execute the query
                cur.execute(insert_query, rows_to_insert)
                
                # Commit the transaction for this row
                tar_conn.commit()
                successful_inserts += 1
                print(f"Inserted row: ")
                
            except Exception as e:
                # Rollback and continue on failure
                import traceback
                print(f"error trans: {str(e)}\n{traceback.format_exc()}\n ")
                tar_conn.rollback()
                failed_inserts += 1
                # print(f"Error inserting row: {str(e)}")
                # print(f"Failed row: ")
                continue

    print(f"Successfully loaded {successful_inserts} rows into {table_name}")
    print(f"Failed to insert {failed_inserts} rows")