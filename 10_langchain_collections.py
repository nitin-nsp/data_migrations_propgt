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
        pg_collection = get_table_data("langchain_pg_collection",src_db_name="src_pgvector_db")
        
        collection_id={}
        res=[]
        with open("./data/not_uuid_to_project_id.json") as f:
            project_id_json= json.load(f)
        for row in pg_collection:
            
            project_name=row["name"]
            if row["name"].startswith("chroma-databases/"):
                project_name= project_name[len("chroma-databases/"):]
            
            project_id= project_id_json.get(project_name, None)
            
            if not project_id:
                collection_id[row["uuid"]]=row["uuid"]
                continue
            
            res.append({
            "name":str(project_id),
            "cmetadata":row["cmetadata"],
            "uuid":row["uuid"],
            })
            
        
        with open("./data/collection_id.json","w") as f:
            json.dump(collection_id,f)
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
            load_data_into_table(tar_conn, table_name="langchain_pg_collection", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
