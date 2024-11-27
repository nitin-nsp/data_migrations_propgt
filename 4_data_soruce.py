import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid
from dotenv import load_dotenv
load_dotenv()
src_db = 
tar_db = "tar_progpt_db"

# data_source = {
#     "id"
#     "data_url"
#     "name"
#     "is_crawled"
#     "type"
#     "confluence_username"
#     "confluence_api_key"
#     "is_fetch_attachments_enabled"
#     "is_deleted"
#     "updated_at"
#     "created_at"
#     "project_id"
# }


def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        
        with open('./data/not_uuid_to_project_id.json', 'r') as fp:
            not_uuid_to_project_id = json.load(fp)

        project_data = get_table_data("accounts_projects")
        users_data = get_table_data("auth_user", src_db_name=tar_db)

        # table(project_data[0])
        js_data = json.loads(project_data[0]["media"])
        # print(js_data, type(js_data))
        res = []
        id_cnt = 1
        project_id_to_data_source_id={}
        data_source_to_project_id={}
        
        
        for row in project_data:

            user_id=row["user_id"]
            project_name=row["project_id"]
            # query = """
            #         SELECT id FROM projects_project WHERE user_id = %s AND name = %s
            #         """
            # project_id = get_query_result(db_name=tar_db, query=query,  params=(user_id, project_name))
            # if not project_id: continue
            
            # project_id=project_id[0]["id"]
            
            project_id=not_uuid_to_project_id.get(row["not_uuid"], None)
            if not project_id: continue
          
    
            media_dict = json.loads(row["media"])

            # urls
            for item in media_dict["urls"]:
                res.append({
                    "id":id_cnt,
                    "data_url":item["file_path"][0],
                    "name":item["file_path"][0],
                    "is_crawled":True,
                    "type":"webUrl",
                    "confluence_username":None,
                    "confluence_api_key":'',
                    "is_fetch_attachments_enabled":item["file_path"][1],
                    "is_deleted":False,
                    "updated_at":item["updated_at"],
                    "created_at":row["timestamp"],
                    "project_id":project_id
                })
                if project_id not in project_id_to_data_source_id:
                        project_id_to_data_source_id[project_id] = []
                project_id_to_data_source_id[project_id].append(id_cnt)
                
                data_source_to_project_id[id_cnt]=project_id
                
                id_cnt+=1
                
            
            
            for item in media_dict["pdf_files"]:
                new_data_url=item["file_path"].split("/")[-1]
                
                res.append({
                    "id":id_cnt,
                    "data_url":f"{project_id}/{new_data_url}",
                    "name":item["file_path"].split('/')[-1],
                    "is_crawled":True,
                    "type":"uploads",
                    "confluence_username":None,
                    "confluence_api_key":'',
                    "is_fetch_attachments_enabled":False,
                    "is_deleted":False,
                    "updated_at":item["updated_at"],
                    "created_at":row["timestamp"],
                    "project_id":project_id,
                })
                if project_id not in project_id_to_data_source_id:
                        project_id_to_data_source_id[project_id] = []
                project_id_to_data_source_id[project_id].append(id_cnt)
                
                data_source_to_project_id[id_cnt]=project_id
                id_cnt+=1
            
           
            if row["space_key"] and len(row["space_key"])>0:
                for item in row["space_key"]:
                    # space_obj=json.loads(item)
                    # print(item)
                    res.append({
                        "id":id_cnt,
                        "data_url":row["url"],
                        "name":item["space_key"][0],
                        "is_crawled":True,
                        "type":"confluence",
                        "confluence_username":row["confluence_username"],
                        "confluence_api_key":row["api_key"],
                        "is_fetch_attachments_enabled":item["space_key"][1],
                        "is_deleted":False,
                        "updated_at":row["last_updated"],
                        "created_at":row["timestamp"],
                        "project_id":project_id
                    })
                    if project_id not in project_id_to_data_source_id:
                        project_id_to_data_source_id[project_id] = []
                    project_id_to_data_source_id[project_id].append(id_cnt)
                    
                    data_source_to_project_id[id_cnt]=project_id
                    id_cnt+=1
        
        
        with open('./data/data_source_to_project_id.json', 'w') as fp:
            json.dump(data_source_to_project_id, fp)

        with open('./data/project_id_to_data_source_id.json', 'w') as fp:
            json.dump(project_id_to_data_source_id, fp)
        
        
        return res

    except Exception as e:
        print(f"error trans: {str(e)}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        # table(data)

        # save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data_into_table(tar_conn, table_name="projects_datasource", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
