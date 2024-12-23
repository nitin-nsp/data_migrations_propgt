import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "src_progpt_db"
tar_db = "tar_progpt_db"

# data_source = {
#    id 
# logo_image_url
# background_image_url
# theme_color
# custom_persona
# chatbot_ending_msg
# custom_prompt
# is_progpt_branding_enabled
# is_citation_show_enabled
# citation_view_type
# is_sharing_enabled
# is_memorization_enabled
# is_gpt_response_enabled
# chatbot_dimension_data
# project_id

# }


def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        with open('data/not_uuid_to_project_id.json') as f:
            not_uuid_to_project_id = json.load(f)

        user_exp_data = get_table_data("accounts_userexperience")
        users_data = get_table_data("auth_user", src_db_name=tar_db)

        fail_project_id, fail_project_query,fail_project_name=0,0,0
        res = []
        id_cnt = 1
        for row in user_exp_data:
            # user=''
            # for p_user in users_data:
            #     email=p_user["email"]
            #     if row["primary_key"].startswith(email):
            #         user=p_user
            #         break
            
            # if not user:continue
            # print(row["primary_key"],"=======",user["email"])
            
            project_id=not_uuid_to_project_id[row["primary_key"]]
            if not project_id:
                fail_project_id+=1
                continue
                
            query="""
            select * from projects_project where id=%s
            """
            project_query=get_query_result(query=query,db_name=tar_db,params=(project_id,))
            if not project_query:
                fail_project_query+=1
                continue
            
            project_id=project_query[0]["id"]
            query="""
            select * from accounts_projects where not_uuid=%s
            """
            project_name=get_query_result(query=query,db_name=src_db,params=(row["primary_key"],))
            
            if  not project_name:
                fail_project_name+=1
                # print("::::::::::::::::::::::",row["primary_key"])
                # break
                
            # print("::::::::::::::::::::::",project_name[0]["enable_share"])    
            res.append({
                "id": id_cnt,
                "logo_image_url": row["logo_image"],
                "background_image_url":row["background_image"],
                "theme_color": row["chatbot_color"],
                "custom_persona": row["prompt"],
                "chatbot_ending_msg": row["bot_ending_msg"],
                "custom_prompt": row["custom_prompt"],
                "is_progpt_branding_enabled": row["branding"],
                "is_citation_show_enabled": row["citation_view"],
                "citation_view_type": "always-hide",
                "is_sharing_enabled": project_name[0]["enable_share"] if project_name else False,
                "is_memorization_enabled": row["memorizing"] if row["memorizing"] else False,
                "is_gpt_response_enabled": row["gpt_response"] if row["gpt_response"] else False,
                "chatbot_dimension_data": row["embed_data"],
                "project_id": project_id,

            })
            
            id_cnt+=1
                
                
        print(f"fail_project_id: {fail_project_id} \n fail_project_query: {fail_project_query} \n fail_project_name: {fail_project_name}")        
        return res
            

    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        
        # save in json
        import csv
        with open('project_setting.csv', 'w', newline='') as fp:
            writer = csv.DictWriter(fp, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        # print(data)
        # table(data)

        # save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="projects_projectsetting", data=data)

        # print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
