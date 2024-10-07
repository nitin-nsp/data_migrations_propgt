        
import json
from utils import *

def transform_data():
    """Transform the data as needed.
    Profile table
    """
    data=get_table_data("accounts_customuser")
    data.sort( key=lambda x:x["id"])
    res=[]
    username_set=set()
    for row in data:
         if row["username"] not in username_set:  # Check if the username is already in the set
            username_set.add(row["username"])
            res.append({
                "id":row["id"],
                "password":row["password"],
                "last_login":row["last_login"],
                "is_superuser":row["is_superuser"],
                "username":row["username"],
                "first_name":row["first_name"],
                "last_name":row["last_name"],
                "email":row["email"],
                "is_staff":row["is_staff"],
                "is_active":row["is_active"],
                "date_joined":row["date_joined"]
            })
    
    #remove duplicate username
    # print(res)
        
    return res


def run():

    try:
        
        # transform
        data = transform_data()
        # table(data)
        
        
        #save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data(tar_conn, table_name="auth_user", data=data)
        
        
        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
