from datetime import datetime, timezone, timedelta
import pytz 
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from connection import connect_to_db,extract_data
from load_data import load_data
from table import  table


def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        UTC = pytz.utc  
        src_db_name='src_progpt_db'
        with connect_to_db(db_name=src_db_name) as src_conn:
            
            signup_data=extract_data(src_conn,table_name="accounts_signup")
        res=[]
        cnt_id=1
        plan_id=["free","standard","premium","enterprise"]
        
        for row in signup_data:
            sub_id=plan_id.index(row["subs_name"])+1 if row["subs_name"] in plan_id else None
            res.append({
                "id":cnt_id,
                "updated_at": datetime.now(timezone.utc),
                "address":row["address"],
                "billing_address":"",
                "profile_image_url":row["profile"],
                
                "no_of_queries":row["no_of_queries"],
                "no_of_content":row["no_of_content"],
                "no_of_projects":row["no_of_projects"],
                
                "free_plan_used":row["free_plan_used"],
                "plan_expiry_at":row["plan_expiry"],
                "is_plan_expired":row["is_expired"],
                
                "is_canceled_monthly_pay":row["cancel_at_end"],
                "is_query_limit_expired":row["query_limit"],
                "is_changing_plan":row["is_changing_plan"],
                "subscription_id":sub_id,
                "user_id":row["user_id"],
                "can_share_chat":True,
                
            })
            cnt_id+=1
            
        return res
    except Exception as e:
        print(f"error trans: {str(e)}")
        return []


def etl_process(src_db, tar_db, src_table, tar_table):
    """Main ETL process."""
    with connect_to_db(src_db) as src_conn, connect_to_db(tar_db) as tar_conn:
        # Extract
        raw_data = extract_data(src_conn, src_table)
        
        # Transform
        transformed_data = transform_data(raw_data)
        
        # Load
        load_data(tar_conn, tar_table, transformed_data)

def run():
    
    try:
        src_db_name='src_progpt_db'
        tar_db_name='tar_progpt_db'
        
        
        
        src_table_name='accounts_customuser'
        
        
        tar_table_name='auth_user'
        # connect_to_db(tar_db_name)
        data=transform_data()
        table(data[:3])
        # print(data[2]["plan_expiry_at"])
        with connect_to_db(tar_db_name) as tar_conn:
            load_data(tar_conn=tar_conn,table_name="accounts_profile",data=data[:2])
        # print("completed successfully.")
        # etl_process()
        # data=extract_data(conn_src,table_name=table_name)
        
        # print(data[0],type(data))
        # print()
        # print(data)
        
    except Exception as e:
        print(f"error: {str(e)}")
        
        