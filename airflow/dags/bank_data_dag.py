# airflow/dags/bank_data_dag.py
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

with DAG(
    dag_id='bank_data_generation_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['bank', 'etl'],
) as dag:
    generate_bank_data = DockerOperator(
        task_id='generate_bank_data',
        image='arlequin-pyspark-client',
        command=["python3", "/scripts/generate_data.py"],
        network_mode="arlequin_default",
        docker_url="unix://var/run/docker.sock",  # explícito
        api_version="auto",
        mounts=[
        Mount(
            source='scripts_data',  # <- nombre del volumen
            target='/scripts',
            type='volume'
        )
        ],
        #docker_url="unix://var/run/docker.sock",
        auto_remove=False,
        do_xcom_push=False,
        force_pull=False,
        mount_tmp_dir=False,
    )

# # airflow/dags/bank_data_dag.py
# from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator
# from datetime import datetime, timedelta
# from docker.types import Mount

# with DAG(
#     dag_id='bank_data_generation_dag',
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=timedelta(hours=1),
#     catchup=False,
#     tags=['bank', 'etl'],
# ) as dag:
#     generate_bank_data = DockerOperator(
#         task_id='generate_bank_data',
#         image='arlequin-pyspark-client',
#         command=["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/scripts/generate_data.py"],
#         network_mode="arlequin_default",
#         mounts=[
#             Mount(source='C:/Users/ljpca/Downloads/Repositorio/arlequin/scripts', target='/scripts', type='bind'),
#             Mount(source='C:/Users/ljpca/Downloads/Repositorio/arlequin/hadoop-base/hadoop-conf', target='/opt/hadoop/etc/hadoop', type='bind')
#         ],
#         docker_url="unix://var/run/docker.sock",
#         auto_remove=True,
#         do_xcom_push=False,
#         force_pull=False, # Make sure this line is there
#         mount_tmp_dir=False,
#     )