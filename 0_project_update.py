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
            profile_id_to_organization_id = [row for row in csv.DictReader(f)]
            
        res = []
        id_cnt=1
        profile_id_to_organization_id=[]
        for row in profile_id_to_organization_id:
            profile_id=row["profile_id"]
            organization_id=row["org_id"]
            email=row["email"]
            project_id=row["project_id"]
            user_id=row["user_id"]
            
            res.append({
                "id": id_cnt,
                "email": email,
                "role":'organization owner',
                "invitation_accepted":True,
                "assigned_at":str(datetime.now(timezone.utc)),
                "updated_at":str(datetime.now(timezone.utc)),
                "organization_id": organization_id,
                "project_id": project_id,
                "user_id":profile_id
            })
            id_cnt+=1
            
        
        with open('./data/profile_id_to_organization_id.json', 'w') as fp:
            json.dump(profile_id_to_organization_id, fp)     
                
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
        
        table(data[:1])
        # save in json
        # import csv
      
        # print(data)
        # table(data)

        # save in db load_data_into_table
        with connect_to_db(tar_db) as tar_conn:
            load_data_into_table(tar_conn, table_name="accounts_organization", data=data)

        # print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
