        
import json
from utils import *

def transform_data():
    
    try:
        data=get_table_data("accounts_customuser")
        data.sort( key=lambda x:x["id"])
        res=[]
        # username_set=set()
        user_id_to_email = {}
        email_to_user_id = {}
        # print(data[0])
        for row in data:
            # if row["username"] not in username_set:  # Check if the username is already in the set
            #     username_set.add(row["username"])
            
            first_name, last_name = row["username"],''
            if row["username"]:
                name_list=row["username"].split(' ')
                first_name = name_list[0]
                if len(name_list)>1:
                    last_name = name_list[1]
            # print(first_name,last_name) 
            # break
            res.append({
                "id":row["id"],
                "password":row["password"],
                "last_login":row["last_login"],
                "is_superuser":row["is_superuser"],
                "username":row["email"] if row["username"]!='admin' else 'admin',
                "first_name":first_name,
                "last_name":last_name,
                "email":row["email"],
                "is_staff":row["is_staff"],
                "is_active":row["is_active"],
                "date_joined":row["date_joined"]
            })
            user_id_to_email[row["id"]] = row["email"]
            email_to_user_id[row["email"]] = row["id"]
        
        #remove duplicate username
        # print(res)

        with open('./data/user_id_to_email.json', 'w') as fp:
            json.dump(user_id_to_email, fp)
        with open('./data/email_to_user_id.json', 'w') as fp:
            json.dump(email_to_user_id, fp)
        
        return res
        
    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        print(f"error: {str(e)}")


def run():

    try:
        
        # transform
        data = transform_data()
        # print(data)
        # table(data)
        
        
        #save in db
        with connect_to_db('tar_sg_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="auth_user", data=data)
        
        
        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
