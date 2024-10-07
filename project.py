from datetime import datetime, timezone, timedelta
import pytz 
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from connection import connect_to_db,extract_data
from load_data import load_data
from table import  table
import uuid

def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        UTC = pytz.utc  
        src_db_name='src_progpt_db'
        with connect_to_db(db_name=src_db_name) as src_conn:
            
            project_data=extract_data(src_conn,table_name="accounts_projects")
        res=[]
        
        for row in project_data:
             res.append({
                "id":str(uuid.uuid4()),
                "name": row["project_id"],
                "chroma_db_path":row["chroma_db_path"],
                "data_retention":row["data_retention"],
                "updated_at":row["last_updated"],
                "created_at":row["timestamp"],
                
                "user_id":row["user_id"],
               
                
            })
                       
        return res
    except Exception as e:
        print(f"error trans: {str(e)}")
        return []



def run():
    
    try:
        src_db_name='src_progpt_db'
        tar_db_name='tar_progpt_db'
        
        
        
        src_table_name='accounts_customuser'
        
        
        tar_table_name='projects_project'
        # connect_to_db(tar_db_name)
        data=transform_data()
        # table(data[:3])
        # print(data[2]["plan_expiry_at"])
        with connect_to_db(tar_db_name) as tar_conn:
            load_data(tar_conn=tar_conn,table_name="projects_project",data=data)
        # print("completed successfully.")
        # etl_process()
        # data=extract_data(conn_src,table_name=table_name)
        
        # print(data[0],type(data))
        # print()
        # print(data)
        
    except Exception as e:
        print(f"error: {str(e)}")
        

if __name__ == "__main__":
    run()