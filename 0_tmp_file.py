import json,csv
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "stage_progpt_db"
tar_db = "stage_progpt_db"




def transform_data():
    
    try:
        

        
        with open('data/project_email_profile.csv', 'r') as f:
            reader = csv.DictReader(f)
            profile_id_to_organization_id = [row for row in reader if reader.fieldnames]
            
        
        res = []
        
        
        # print(profile_id_to_organization_id)
        for row in profile_id_to_organization_id:
            res.append({
                "project_id": row["project_id"],
                "org_id": row["org_id"]
            })
            
        
             
                
        # print(f"fail_project_id: {fail_project_id} \n fail_project_query: {fail_project_query} \n fail_project_name: {fail_project_name}")        
        return res
            

    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        
        print(data)
        # save in json
        # import csv
      
        # print(data)
        # table(data)

        # save in db load_data_into_table
        with connect_to_db(tar_db) as tar_conn:
            load_data_into_table(tar_conn, table_name="temp_project_orgs", data=data)

        # print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
