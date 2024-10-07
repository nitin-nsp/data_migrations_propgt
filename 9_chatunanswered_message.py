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
        chat_unanswered = get_table_data("accounts_unanswered")
        res=[]
        id_cnt=1
        for row in chat_unanswered:
            
            with open("not_uuid_to_project_id.json") as f:
                project_id_json = json.load(f)
                # print(chat_conversations_id_map)
            project_id= project_id_json.get(row["not_uuid"], None)
            
            if not project_id: continue
            
            res.append({
            "id": id_cnt,
            "question": row["question"],
            "project_id": project_id,
            })
            id_cnt+=1
        
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
            load_data(tar_conn, table_name="chatbot_chatunanswered", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
