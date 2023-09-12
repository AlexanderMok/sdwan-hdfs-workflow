# DAG scheduler on Apache airflow

## Purpose

用作定时任务。可使用python建构具有上下依赖关系的一系列操作，灵活建构DAG(Directed Acyclic Graph)，定时运行指定任务

## Requirement

- Python3
- Apache airflow 1.10.5
  - [github download address](https://github.com/apache/airflow/releases)

## Installation

- Prepare python3 environment

```shell
sudo apt-get install python3-pip

sudo pip3 install virtualenv

# set python3 virtual env in directory 'myenv'
virtualenv -p python3 ./myenv

# activate python3
source ./myenv/bin/activate
```

- Install Apache airflow

```shell

tar -zxf apache-airflow-1.10.5.tar.gz

cd apache-airflow-1.10.5

# ensure python3 is activated
python setup.py install
```

## Configuration

- change timezone of DAGs

Timezone on UI cannot be changed. Default GMT+0
```
vim airflow.cfg

default_timezone = Asia/Shanghai
```

- Database

目前使用默认的sqlite数据库，故而只能单线程调度，不能使用并行调度

以后若有需要更改，需参考官方文档

## Start

- Execute shell
```shell
./start-webserver-scheduler.sh
```

需启动`webserver`以及调度器`scheduler`

```shell
#!/bin/sh

# dir of python3 env
env=/opt/sdwan_airflow_env/
source /opt/sdwan_airflow_env/bin/activate
cd $env
echo $(pwd)
PORT=5000
bin/airflow webserver -D -p $PORT
bin/airflow scheduler -D
echo $(ps -ef | grep airflow)
```

## DAG

- Write a DAG in python
- Put it under directory `dags`
- Fresh `webserver UI` on browser

- Example of using `for` to generate `one-to-many` tasks
```python
from airflow.operators.bash_operator import BashOperator
for i in range(289):
    task = BashOperator(
        task_id="extraction_time_point_" + str(i),
 bash_command="hdfs dfs -ls -d -h -q -t /flume/sdn/wan/{}/{}/002/**".format("", str(i)),
        bash_command="echo {}".format(i),
        dag=dag
   )
    task2 >> task
    task >> task3
```

- One full example

```python
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'xxx',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': True,
}

# BranchPython operator that depends on past
# and where tasks may run or be skipped on
# alternating runs
dag = DAG(
    dag_id='example_branch_dop_operator_v5',
    schedule_interval=None,
    default_args=args,
)


def should_run(**kwargs):
    print('------------- exec dttm = {} and minute = {} and day = {}'.
          format(kwargs['execution_date'], kwargs['execution_date'].minute, kwargs['execution_date'].day))
    if kwargs['execution_date'].minute % 2 == 0:
        return ["dummy_task_1", "dummy_task_3"]
    else:
        return "dummy_task_2"


cond = BranchPythonOperator(
    task_id='condition',
    provide_context=True,
    python_callable=should_run,
    dag=dag,
)

dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
dummy_task_3 = DummyOperator(task_id='dummy_task_3', dag=dag)
dummy_task_4 = DummyOperator(task_id='dummy_task_4', dag=dag)

cond >> [dummy_task_1, dummy_task_2, dummy_task_3]
dummy_task_3 >> dummy_task_4
```