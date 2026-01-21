def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MRPAM"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_database"

def test_dags_integrity(dagbag):
    assert dagbag.import_errors == {}, f"Import errors found in DAGs: {dagbag.import_errors}"
    print("=========================")
    print(dagbag.import_errors)
    expected_dag_ids = ["youtube_video_stats_dag", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("=========================")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} not found in loaded DAGs"

    assert dagbag.size() == 3
    print("=========================")
    print(dagbag.size())

    expected_task_counts = {
        "youtube_video_stats_dag": 4,
        "update_db": 2,
        "data_quality": 2
    }
    print("=========================")

    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} expected {expected_count} tasks, found {actual_count}"
        print(dag_id, len(dag.tasks))
