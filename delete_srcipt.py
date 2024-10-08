import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "src_progpt_db"
tar_db = "tar_progpt_db"
import logging

def delete_query_result(query,db_name,params=None):
    try:
        with connect_to_db(db_name=db_name) as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                
                conn.commit()
                print("deleted successfully.")
    except Exception as e:
        conn.rollback()
        print (f"error (get query) {str(e)}")
 
        

def run():

    try:

        with open("./data/collection_id.json") as f:
            collection_id = json.load(f)
            
        for k,v in collection_id.items():
            
            query="""
            delete from langchain_pg_embedding where uuid=%s
            """
            result= delete_query_result(query=query,db_name="tar_vector_db",params=(v,))
            print(result)
            
            
            
            
        
    except Exception as e:
        logging.error(f"error: {str(e)}")


if __name__ == "__main__":
    run()

