import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "src_progpt_db"
tar_db = "tar_progpt_db"


data={
#     id 
# created_at
# updated_at
# conversation_name
# name_show
# is_share_enabled
# project_id
# user_id

}

def transform_data():
    """Transform the data as needed.
    chatconversations table
    """
    try:
        pg_embedding = get_table_data("langchain_pg_embedding",src_db_name="src_pgvector_db")
        
        res=[]
        
        for row in pg_embedding:
            
            
            res.append({
            "collection_id":row["collection_id"],
            "embedding":row["embedding"],
            "document":row["document"],
            "cmetadata":row["cmetadata"],
            "custom_id":row["custom_id"],
            "uuid":row["uuid"],
            })
            
        
        return res
        
    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        # table(data)

        # save in db
        with connect_to_db('tar_vector_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="langchain_pg_embedding", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
