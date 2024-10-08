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
        chat_data = get_table_data("accounts_chatmessages")
        res=[]
        
        
        with open("./data/chat_conversations_id_map.json") as f:
            chat_conversations_id_map = json.load(f)
        for row in chat_data:
                # print(chat_conversations_id_map)
            conversation_id = chat_conversations_id_map.get(str(row["conversation_id"]), None) or chat_conversations_id_map.get(row["conversation_id"], None)
            if not conversation_id:
                continue
            # print(conversation_id)
            query = """
            select * from chatbot_chatconversation where id = %s
            """
            chatbot_conversation = get_query_result(db_name=tar_db, query=query, params=(conversation_id,))
            if not chatbot_conversation:
                continue
            chatbot_conversation = chatbot_conversation[0]
            
            
                
            res.append({
                "id": str(uuid.uuid4()),
                "user_message": row["user_message"],
                "bot_message": str(row["bot_message"]) if not row["bot_message"] else "",
                "created_at": chatbot_conversation["created_at"] if not chatbot_conversation["created_at"] else datetime.now(tz=pytz.utc).isoformat(),
                "conversation_id": conversation_id,
            })
            
           
            
        
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
            load_data_into_table(tar_conn, table_name="chatbot_chatmessage", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
