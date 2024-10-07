import json
import pytz
from utils import *
from datetime import datetime, timezone, timedelta
import uuid

src_db = "src_progpt_db"
tar_db = "tar_progpt_db"




def transform_data():
    """Transform the data as needed.
    Profile table
    """
    try:
        UTC = pytz.utc

        Events={
            "created":"created",
            "renamed":"renamed",
            "deleted":"deleted",
            "settings":"settings",
            "re-crawled":"recrawled",
        }
        # users_data = get_table_data("auth_user", src_db_name=tar_db)
        # project_data= get_table_data("projects_project", src_db_name=tar_db)
        
        media_data= get_table_data("accounts_media")
        id_cnt=1
        res=[]
        for row in media_data:
            # if row["status"]=="created": continue
            # not_uuid =email+project_name
            email=row["not_uuid"][:len(row["not_uuid"])-len(row["project_name"])]
           
            query="""
            select * from projects_project where name=%s and user_id in (select id from auth_user where email=%s)
            """
            project_query=get_query_result(query=query,db_name=tar_db,params=(row["project_name"],email))
            # print(project_query)
            if not project_query: continue
            
            project_id=project_query[0]["id"]
            project_query=project_query[0]
            
            
            
            if row["project_type"]=="Confluence":
                data_url,space_key=row["media"].split("/spaces/")
                
                if not space_key or not data_url: continue
                query="""
                select * from projects_datasource where data_url=%s and name=%s and project_id=%s
                """
                data_source_query=get_query_result(query=query,db_name=tar_db,params=(data_url,space_key,project_id))

                if not data_source_query: continue
                # print(f"confulence: {data_source_id}")
                
            else:   
                if row["project_type"]=="PDF":
                    media_name=row["media"]
                elif row["project_type"]=="URL":
                    media_name=row["media"][0]
                    
                query="""
                select * from projects_datasource where name=%s and project_id=%s
                """
                data_source_query=get_query_result(query=query,db_name=tar_db,params=(media_name,project_id))
                
                if not data_source_query: continue
                # print(f"{project_id} => {data_source_id}")  
                
            
            for data_source in data_source_query:
                
                data_source_id=data_source["id"]
                event_type=Events[row["status"]]
                message=''
                if event_type=='created':
                    if row["project_type"]=="Confluence":
                        message=f'Confluence with URL {data_source["name"]} was integrated'
                    elif row["project_type"]=="PDF":
                        message=f'{data_source["name"]} was uploaded'
                    else:
                        message=f'{data_source["name"]} is integrated'
                elif event_type=='deleted':
                    message = f'{data_source["name"]} was deleted from project {project_query["name"]}'
                elif event_type=='recrawled':
                    # print(f"recrawled: {data_source_id} => {data_source} ==> {project_query}")
                    message = f'{data_source["name"]} was re-crawled from project {project_query["name"]}'
                elif event_type=='renamed':
                    message = f'Project {project_query["name"]} was Re-named'
                elif event_type=='settings':
                    message = (
                    f'{project_query["name"]} Project settings were updated'
                )
                else:
                    message = f'{data_source["name"]} is {event_type}'

                res.append({
                    
                    "id":id_cnt,
                    "event_type":event_type,
                    "message":message,
                    "created_at":data_source["created_at"],
                    "updated_at":row["updated_at"],
                    "data_source_id":data_source_id,
                    "project_id":project_id,


                })
                id_cnt+=1
                
                
                 
            
            
        
        # print(res[0])
        # table(project_data[0])
        return res

    except Exception as e:
        import traceback
        print(f"error trans: {str(e)}\n{traceback.format_exc()}")
        # print(f"error trans: {str(e)}")
        return []


def run():

    try:

        # transform
        data = transform_data()
        # table(data)

        # save in db
        with connect_to_db('tar_progpt_db') as tar_conn:
            load_data(tar_conn, table_name="projects_eventlog", data=data)

        print("success ~~~~ !!!!")
    except Exception as e:
        print(f"error: {str(e)}")


if __name__ == "__main__":
    run()
