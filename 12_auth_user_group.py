        
import json
from utils import *

def transform_data():
    """Transform the data as needed.
    Profile table
    """
    data=get_table_data("accounts_customuser_groups")
    res=[]
    for row in data:
        res.append({
            "id":row["id"],
            "user_id":row["customuser_id"],
            "group_id":row["group_id"],
        })
    
    return res
         
    
    


def run():

    try:
        
        # transform
        data = transform_data()
        # table(data)
        
        
        #save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="auth_user_groups", data=data)
        
        
        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()