import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

DB_PARAMS = {
    'host': 'localhost',
    'port': '5011',
    'user': 'postgres',
    'password': 'mysecretpassword'
}

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
