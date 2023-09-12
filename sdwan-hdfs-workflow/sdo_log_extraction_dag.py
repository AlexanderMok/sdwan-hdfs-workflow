# -*- coding: utf-8 -*-
from datetime import timedelta, datetime
from operator import itemgetter
import logging
import os.path
import requests
import subprocess
import shutil
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'xxx',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    dag_id="shda_sdwan_log_operator",
    default_args=default_args,
    description="A log extraction DAG for customer sdo",
    schedule_interval="0 2 * * *",  # 2am each day
    # schedule_interval=None,
)

service_args = {
    "customer": "sdo",
    "service_id": 18,
    "condition": "LY-CBOX-SHDA-01",
    # "condition": "LY-CBOX-DINYING-01",
    "parent_path": "hdfs://cluster/flume/sdn/wan",
    "sep": ",",
    "local_path": "/data/bddata/sdo/local",
    "cloud_path": "/data/bddata/sdo/cloud",
    # "post_server": "http://192.168.129.116:1080",
    "post_server": "http://10.4.13.147:1080 ",
    "post_api": "/log_sync/1_0/customer_log",

}


def list_files(dir, suffix, prefix=None):
    d = {}
    for root, dirs, files in os.walk(dir):
        for f in files:
            path = os.path.join(root, f)
            if not prefix and path.endswith(suffix):
                d[path] = int(os.path.getmtime(path))
            if prefix and f.startswith(prefix) and f.endswith(suffix):
                d[path] = int(os.path.getmtime(path))
    res = sorted(d.items(), key=itemgetter(1))
    return list(map(lambda x: x[0], res))


def _prepare_move_path(local_filename_zip, yesterday):
    if os.path.exists(local_filename_zip):
        path = os.path.join(service_args["cloud_path"],
                            service_args["customer"],
                            str(service_args["service_id"]),
                            str(yesterday.year),
                            str(yesterday.month))
        if not os.path.exists(path):
            os.makedirs(path, 644)
        return path


def _move(local_filename_zip):
    if os.path.exists(local_filename_zip):
        yesterday = (datetime.today() - timedelta(days=1))

        path = _prepare_move_path(local_filename_zip, yesterday)

        basename = os.path.basename(local_filename_zip)
        cloud_output_filename = os.path.join(path, basename)

        if os.path.exists(cloud_output_filename):
            os.remove(cloud_output_filename)

        logging.info("Moving file from [{}] to [{}]".format(local_filename_zip, cloud_output_filename))

        shutil.copyfile(local_filename_zip, cloud_output_filename + ".tmp")
        shutil.move(cloud_output_filename + ".tmp", cloud_output_filename)
        os.remove(local_filename_zip)

        post_path = os.path.join(service_args["customer"],
                                 str(service_args["service_id"]),
                                 str(yesterday.year),
                                 str(yesterday.month),
                                 basename)
        return post_path, cloud_output_filename
    else:
        raise IOError("{} dose not exists.".format(local_filename_zip))


def _post(url, data):
    try:
        logging.info("Post url [{}]. Data [{}].".format(url, data))
        resp = requests.post(url=url, json=data)
        logging.info("{}. Content [{}]".format(resp, resp.content))
    except Exception as e:
        logging.error("Post error. {}".format(e.message), e)
    else:
        logging.info("Post result for log sync. Status {}, Context {}.".format(resp.status_code, resp.content))


def extract():
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    parent_path = service_args["parent_path"]
    condition = service_args["condition"]
    output_filename = os.path.join(
        service_args["local_path"],
        "{}_".format(service_args["customer"]) + yesterday + "_log")
    cmd_ls = "hdfs dfs -ls {}/{}/*/002/* | awk -F' ' ".format(parent_path, yesterday) + " '{" + "print $8" + "}'"
    cmd = cmd_ls + " | xargs hdfs dfs -text | grep {} >> {}".format(
        condition,
        output_filename)
    logging.info("Execute command [{}]".format(cmd))
    subprocess.call(cmd, shell=True)
    return output_filename


def compress_day(**context):
    local_filename = context["task_instance"].xcom_pull(task_ids="content_extraction")
    zip_file = local_filename + ".gz"
    if os.path.exists(local_filename):
        cmd = """cd {}; gzip -c {} > {}""".format(service_args["local_path"], local_filename, zip_file)
        logging.info("Execute command [{}]".format(cmd))
        subprocess.call(cmd, shell=True)
    else:
        raise IOError("{} dose not exists.".format(local_filename))


def move_day(**context):
    local_filename = context["task_instance"].xcom_pull(task_ids="content_extraction")
    local_filename_zip = local_filename + ".gz"
    post_path, cloud_file_path = _move(local_filename_zip)
    return post_path, cloud_file_path


def post_file_day(**context):
    post_path, cloud_file_path = context["task_instance"].xcom_pull(task_ids="file_move_day")
    if os.path.exists(cloud_file_path):
        url = service_args["post_server"] + service_args["post_api"]
        yesterday = (datetime.today() - timedelta(days=1))
        data = {
            "year": yesterday.year,
            "month": yesterday.month,
            "day": yesterday.day,
            "customer": service_args["customer"],
            "service_id": service_args["service_id"],
            "type": 1,
            "path": post_path
        }
        _post(url, data)


def compress_month(**context):
    file_list = list_files(dir=service_args["local_path"], suffix="log")
    if len(file_list) > 0:
        month = (datetime.today() - timedelta(days=1)).strftime("%Y%m")
        local_filename_zip = os.path.join(
            service_args["local_path"],
            "{}_".format(service_args["customer"]) + month + "_log.gz")
        for i in file_list:
            cmd = """cd {}; gzip -c {} >> {}""".format(service_args["local_path"], i, local_filename_zip)
            logging.info("Execute command [{}]".format(cmd))
            subprocess.call(cmd, shell=True)
            # remove origin log
            os.remove(i)
        return local_filename_zip


def move_month(**context):
    local_filename_zip = context["task_instance"].xcom_pull(task_ids="file_compression_month")
    post_path, cloud_file_path = _move(local_filename_zip)
    return post_path, cloud_file_path


def post_file_month(**context):
    post_path, cloud_file_path = context["task_instance"].xcom_pull(task_ids="file_move_month")
    if os.path.exists(cloud_file_path):
        url = service_args["post_server"] + service_args["post_api"]
        yesterday = (datetime.today() - timedelta(days=1))
        data = {
            "year": yesterday.year,
            "month": yesterday.month,
            "customer": service_args["customer"],
            "service_id": service_args["service_id"],
            "type": 2,
            "path": post_path
        }
        _post(url, data)


def should_run_month(**kwargs):
    """
    Return next branching. If it is multiple branching, return list of task_id. The last task in this list run first.
    eg. task 'file_move_day' and its children tasks run before 'file_compression_month' and its children tasks.
    """
    execution_date = datetime.today()
    logging.info("Checking should_run_month. Execution_date on UTC+0 is [{}], on UTC+8 is [{}]".format(kwargs["execution_date"], execut
ion_date))
    if execution_date.day == 1:
        return ["file_compression_month", "file_move_day"]
    else:
        return "file_move_day"


task0 = DummyOperator(
    task_id="dummy_start",
    dag=dag
)

task1 = PythonOperator(
    task_id="content_extraction",
    python_callable=extract,
    dag=dag
)

task2 = PythonOperator(
    task_id="file_compression_day",
    python_callable=compress_day,
    provide_context=True,
    dag=dag
)

task_condition = BranchPythonOperator(
    task_id="check_month",
    provide_context=True,
    python_callable=should_run_month,
    dag=dag,
)

task3 = PythonOperator(
    task_id="file_move_day",
    python_callable=move_day,
    provide_context=True,
    dag=dag
)

task4 = PythonOperator(
    task_id="filepath_post_day",
    python_callable=post_file_day,
    provide_context=True,
    dag=dag
)

task5 = PythonOperator(
    task_id="file_compression_month",
    python_callable=compress_month,
    provide_context=True,
    dag=dag
)

task6 = PythonOperator(
    task_id="file_move_month",
    python_callable=move_month,
    provide_context=True,
    dag=dag
)

task7 = PythonOperator(
    task_id="filepath_post_month",
    python_callable=post_file_month,
    provide_context=True,
    dag=dag
)

"""
Run `python sdo_log_extraction_dag.py` to check static errors
Run `airflow test dag_id task_id date` to test tasks
"""
task0 >> task1 >> task2 >> task_condition >> task3 >> task4
task_condition >> task5 >> task6 >> task7
