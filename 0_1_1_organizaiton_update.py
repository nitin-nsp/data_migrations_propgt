import json,csv
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "stage_progpt_db"
tar_db = "stage_progpt_db"




def transform_data():
    
    try:
        

        
        profile_data = get_table_data("accounts_organization")
            
        
        res = []
        profile_id_to_organization_id=[]
        for row in profile_data:
            # organization_id=str(uuid.uuid4())
            # profile_id=row["profile_id"]
            res.append({
                "id": row["id"],
                "name": row["name"].replace('"s', "'s"),
                "owner":row["owner"],#profile id
                "logo":"",
                "created_at":row["created_at"],
                "updated_at":row["updated_at"],
                "model_status": [row["model_status"]]
            })
            # profile_id_to_organization_id.append({
            #     "profile_id":row["id-2"],"organization_id": organization_id,"email":row["email"],"user_id":row["id"]
            # })
            
        
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
        
        table(data[:1])
        # save in json
        # import csv
      
        # print(data)
        # table(data)

        # save in db load_data_into_table
        # with connect_to_db(tar_db) as tar_conn:
        #     load_data_into_table(tar_conn, table_name="accounts_organization", data=data)

        # print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
