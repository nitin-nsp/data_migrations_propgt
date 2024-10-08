import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "src_progpt_db"
tar_db = "tar_progpt_db"


data={
#     id 
# created_at
# updated_at
# conversation_name
# name_show
# is_share_enabled
# project_id
# user_id

}

def transform_data():
    """Transform the data as needed.
    chatconversations table
    """
    try:
        with open('./data/not_uuid_to_project_id.json', 'r') as fp:
            not_uuid_to_project_id = json.load(fp)
            
            
        chat_data = get_table_data("accounts_chatconversations")
        fail_project_id=0
        res=[]
        chat_conversations_id_map={}
        for row in chat_data:
            
           
            # query="""
            # select email from auth_user where id=%s
            # """
            # email=get_query_result(db_name=tar_db,query=query,params=(row["user_id"],))
        
            # if not email: continue
            # email=email[0]["email"]
            # print(email)
            #project= email+project_name
            # project_name=row["project"][len(email):]
            # if not project_name:
            #     project_name=row["project"]
            
            # print(email,"=======",project_name,"=======",row["user_id"])
            
            # query="""
            # select * from projects_project where name=%s and user_id = %s
            # """
            # project_query=get_query_result(query=query,db_name=tar_db,params=(project_name,row["user_id"]))
            
            project_id=not_uuid_to_project_id[row["project"]]
            if not project_id:
                fail_project_id+=1
                continue
            # print("::::::",project_id," +++++ ",project_name)
            new_uuid=str(uuid.uuid4())
            res.append({
                    "id":new_uuid,
                    "created_at":row["created_at"],
                    "updated_at":row["updated_at"],
                    "conversation_name":row["conversation_name"],
                    "name_show":row["name_show"],
                    "is_share_enabled":row["enable_share"],
                    "user_id":row["user_id"],
                    "project_id":project_id,
            })
            chat_conversations_id_map[row["id"]] = new_uuid
            
        
        
        with open("./data/chat_conversations_id_map.json","w") as f:
            json.dump(chat_conversations_id_map,f)

        print(f"fail_project_id : {fail_project_id}")
        return res
        
    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        # table(data)

        # save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="chatbot_chatconversation", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
