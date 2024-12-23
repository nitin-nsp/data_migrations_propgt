        
import json
import pytz 
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        UTC = pytz.utc  
      
        project_data=get_table_data("accounts_projects")
        res=[]
        not_uuid_to_project_id={}
        project_id_to_not_uuid={}
       
        for row in project_data:
             res.append({
                "id":row["ID"],
                "name": row["project_id"],
                "chroma_db_path":row["chroma_db_path"],
                "data_retention":row["data_retention"],
                "updated_at":row["last_updated"],
                "created_at":row["timestamp"],
                "user_id":row["user_id"],
            })
             project_id_to_not_uuid[row["ID"]] = row["not_uuid"]
             not_uuid_to_project_id[row["not_uuid"]] = row["ID"]
             
        with open('./data/not_uuid_to_project_id.json', 'w') as fp:
            json.dump(not_uuid_to_project_id, fp)
        with open('./data/project_id_to_not_uuid.json', 'w') as fp:
            json.dump(project_id_to_not_uuid, fp)
             
        return res
    except Exception as e:
        print(f"error trans: {str(e)}")
        return []


def run():

    try:
        
        # transform
        data = transform_data()
        # table(data)
        
        
        #save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="projects_project", data=data)
        
        
        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
