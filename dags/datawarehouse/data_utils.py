from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    pg_hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database = "elt_db")
    conn = pg_hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn,cur

def close_cur_conn(cur, conn):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)

    conn.commit()
    close_cur_conn(cur, conn)

def create_table(schema):
    conn,cur = get_conn_cursor()
    
    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        );
        """
    cur.execute(table_sql)
    conn.commit()
    close_cur_conn(cur, conn)

def get_video_ids_from_db(cur,schema):
    cur.execute(f"""SELECT "Video_Id" FROM {schema}.{table};""")
    ids = cur.fetchall()
    video_ids = [row["Video_Id"] for row in ids]
    return video_ids