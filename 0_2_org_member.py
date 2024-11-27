import json,csv
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid
from collections import defaultdict

src_db = "stage_progpt_db"
tar_db = "stage_progpt_db"




def transform_data():
    
    try:
        

        
        with open('data/1_org_id_email.csv', 'r') as f:
            reader = csv.DictReader(f)
            profile_id_to_organization_id = [row for row in reader if reader.fieldnames]
        
            
        
        res = []
        id_cnt=1
        duplicate_constraints_violation = defaultdict(bool)
        for row in profile_id_to_organization_id:
            email=row["email"]
            profile_id=row["profile_id"]
            organization_id=row["org_id"]
            new_entry={
                # "id": id_cnt,
                "email": email,
                "role":'owner',#profile id
                "added_to_organization":str(datetime.now(timezone.utc)),
                "updated_at":str(datetime.now(timezone.utc)),
                "created_at":str(datetime.now(timezone.utc)),
                "invitation_accepted": True,
                "organization_id": organization_id,
                "user_id":profile_id
            
            }
            key=(organization_id,email)
            if key not in duplicate_constraints_violation:
                res.append(new_entry)
                duplicate_constraints_violation[key] = True
            # duplicate_constraints_violation[key].append({email,user_id})
            id_cnt+=1
            
        print(len(res))
        # table(duplicate_constraints_violation)
        # with open('./data/profile_id_to_organization_id.json', 'w') as fp:
        #     json.dump(profile_id_to_organization_id, fp)     
                
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
        
        
        # save in json
        # import csv
      
        # print(data)
        # table(data)

        # save in db load_data_into_table
        with connect_to_db(tar_db) as tar_conn:
            load_data_into_table(tar_conn, table_name="accounts_organizationmember", data=data)

        # print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
