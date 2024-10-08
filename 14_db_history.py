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
        db_hs = get_table_data("message_store",src_db_name="src_db_history")
        
       
        res=[]
        with open("./data/chat_conversations_id_map.json") as f:
            chat_converstions_id_map = json.load(f)
        fail_session_id=0
        fail_session_id_list=[]
        for row in db_hs:
            # print(row)
            # break
            session_id = chat_converstions_id_map.get(str(row["session_id"]), None) 
            if not session_id:
                fail_session_id+=1
                fail_session_id_list.append(row["session_id"])
                continue
            
            message_json=json.dumps(row["message"])
            # print(type(row["message"]))
            # break
            res.append({
                "id":row["id"],
                "session_id":session_id,
                "message":message_json ,
            })
            
            
        
        print(fail_session_id)
        # print(fail_session_id_list)
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
        with connect_to_db('tar_db_history') as tar_conn:
            load_data_into_table(tar_conn, table_name="message_store", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
