from datawarehouse.data_utils import get_conn_cursor, close_cur_conn, create_schema, create_table, get_video_ids_from_db
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_loading import load_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():

    schema = 'staging'
    conn,cur = None,None
    try:
        conn,cur = get_conn_cursor()
        yt_data = load_data()
        create_schema(schema)
        create_table(schema)
        table_video_ids = get_video_ids_from_db(cur,schema)

        for row in yt_data:
            if len(table_video_ids) == 0:
                insert_rows(cur,conn,schema,row)
            else:
                if row["video_id"] in table_video_ids:
                    update_rows(cur,conn,schema,row)
                else:
                    insert_rows(cur,conn,schema,row)
        ids_in_json = {row["video_id"] for row in yt_data}
        ids_to_del = set(table_video_ids) - ids_in_json

        if ids_to_del:
            delete_rows(cur,conn,schema,ids_to_del)
        logger.info(f"{schema} table udated successfully.")
    except Exception as e:
        logger.error(f"Error in updating {schema} table: {e}")
        raise e
    finally:
        if cur and conn:
            close_cur_conn(cur,conn)


@task
def core_table():
    
    schema = 'core'
    conn,cur = None,None
    try:
        conn,cur = get_conn_cursor()
        
        create_schema(schema)
        create_table(schema)
        table_video_ids = get_video_ids_from_db(cur,schema)

        current_video_ids = set()
        cur.execute(f"""SELECT * FROM staging.{table};""")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row["Video_Id"])
            if len(table_video_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur,conn,schema,transformed_row)
            else:
                transformed_row = transform_data(row)
                if transformed_row["Video_Id"] in table_video_ids:
                    update_rows(cur,conn,schema,transformed_row)
                else:
                    insert_rows(cur,conn,schema,transformed_row)
        ids_to_del = set(table_video_ids) - current_video_ids

        if ids_to_del:
            delete_rows(cur,conn,schema,ids_to_del)

        logger.info(f"{schema} table udated successfully.")

    except Exception as e:
        logger.error(f"Error in updating {schema} table: {e}")
        raise e
    finally:
        if cur and conn:
            close_cur_conn(cur,conn)
