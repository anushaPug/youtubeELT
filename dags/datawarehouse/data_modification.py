import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur,conn,schema,row):
    try:
        if schema == 'staging':
            video_id = "video_id"
            cur.execute(f"""INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                        VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);""",
                        row
                        )
        else:
            video_id = "Video_Id"
            cur.execute(f"""INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                        VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);""",
                        row
                        )
        conn.commit()
        logger.info(f"Inserted row with video_id {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with video_id {row[video_id]}: {e}")
        raise e

def update_rows(cur, conn, schema,row):
    try:
        #staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'
        #core
        else:
            video_id = 'Video_Id'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'

        cur.execute(f"""UPDATE {schema}.{table}
                    SET "Video_Title" = %({video_title})s,
                        "Video_Views" = %({video_views})s,
                        "Likes_Count" = %({likes_count})s,
                        "Comments_Count" = %({comments_count})s
                    WHERE "Video_Id" = %({video_id})s AND "Upload_Date" = %({upload_date})s;""",
                    row
                    )
            
        conn.commit()
        logger.info(f"Updated row with video_id {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with video_id {row[video_id]}: {e}")
        raise e
    
def delete_rows(cur, conn, schema, ids_to_del):
    try:
        ids_to_del = f"""({', '.join(f"'{id}" for id in ids_to_del)})"""

        cur.execute(f"""DELETE FROM {schema}.{table} WHERE "Video_Id" IN {ids_to_del};""")
        logger.info(f"Deleted rows with video_ids {ids_to_del}")
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error deleting rows: {e}")
        raise e