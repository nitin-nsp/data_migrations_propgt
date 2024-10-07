# def transform_data(data):
#     """Transform the data as needed."""
    
#     res=[]
#     for row in data:
#         res.append({
#             "id":row["id"],
#             "password":row["password"],
#             "last_login":row["last_login"],
#             "is_superuser":row["is_superuser"],
#             "username":row["username"],
#             "first_name":row["first_name"],
#             "last_name":row["last_name"],
#             "email":row["email"],
#             "is_staff":row["is_staff"],
#             "is_active":row["is_active"],
#             "date_joined":row["date_joined"]
#         })
        
#     return data

# def etl_process(src_db, tar_db, src_table, tar_table):
    # """Main ETL process."""
    # with connect_to_db(src_db) as src_conn, connect_to_db(tar_db) as tar_conn:
    #     # Extract
    #     raw_data = extract_data(src_conn, src_table)
        
    #     # Transform
    #     transformed_data = transform_data(raw_data)
        
    #     # Load
    #     load_data(tar_conn, tar_table, transformed_data)


SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'accounts_profile'
            ORDER BY ordinal_position