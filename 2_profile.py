        
import json
import pytz 
from utils import *
from datetime import datetime, timezone, timedelta


def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        UTC = pytz.utc  
      
        signup_data=get_table_data("accounts_signup")
        res=[]
        cnt_id=1
        plan_id=["free","standard","premium","enterprise"]
        
        for row in signup_data:
            sub_id=plan_id.index(row["subs_name"])+1 if row["subs_name"] in plan_id else None
            plan_expiry=row["plan_expiry"]
            if not row["plan_expiry"] or row["plan_expiry"]==None:
                plan_expiry=str(datetime.now(timezone.utc))
            res.append({
                "id":cnt_id,
                "updated_at": str(datetime.now(timezone.utc)),
                "address":row["address"],
                "billing_address":"",
                "profile_image_url":row["profile"],
                
                "no_of_queries":row["no_of_queries"],
                "no_of_content":row["no_of_content"],
                "no_of_projects":row["no_of_projects"],
                
                "free_plan_used":row["free_plan_used"],
                "plan_expiry_at":plan_expiry,
                "plan_created_at":row["plan_created_at"],
                "is_plan_expired":row["is_expired"],
                
                "is_canceled_monthly_pay":row["cancel_at_end"],
                "is_query_limit_expired":row["query_limit"],
                "is_changing_plan":row["is_changing_plan"],
                "is_account_deleted":False,
                "subscription_id":sub_id,
                "user_id":row["user_id"],
                "can_share_chat":True,
                
            })
            cnt_id+=1
        # table(res)
        return res
    except Exception as e:
        print(f"error trans: {str(e)}")
        return []


def run():

    try:
        
        # transform
        data = transform_data()
        print(len(data))
        # table(data)
        
        
        #save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="accounts_profile", data=data)
        
        
        print("Done.... !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
