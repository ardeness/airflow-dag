from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.providers.mongo.hooks.mongo import MongoHook
from kubernetes.client import models as k8s

namespace = conf.get('kubernetes', 'NAMESPACE') # This will detect the default namespace locally and read the

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

def create_dag(schedule, default_args):
    dag_id = 'simple-dag'
    image = 'hello-world'
    dag = DAG(dag_id, tags=[project], schedule_interval=schedule, default_args=default_args, is_paused_upon_creation=False)

    compute_resources = k8s.V1ResourceRequirements(
        requests={"cpu": "100m", "memory": "100Mi"},
        limits={"cpu": "500m", "memory": "1Gi"}
    )

    with dag:
        KubernetesPodOperator(
            namespace=namespace,
            image = image,
            image_pull_policy='Always',
            cmds = [],
            name="task-"+project+"-",
            task_id="task-"+project+"-",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
        )

    return dag

default_args = {
    'owner': 'None',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


    globals()['simple-dag'] = create_dag(None, default_args)
