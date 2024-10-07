from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
#from airflow.kubernetes.secret import Secret
from kubernetes.client import models as k8s

namespace = conf.get('kubernetes', 'NAMESPACE') # This will detect the default namespace locally and read the

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

def create_dag(schedule, default_args):
    dag_id = 'hycu-subtitle'
    image = 'hello-world'
    project = 'hycu'
    dag = DAG(dag_id, tags=[project], schedule_interval=schedule, default_args=default_args, is_paused_upon_creation=False)

    whisper_compute_resources = k8s.V1ResourceRequirements(
       requests={"cpu": "1000m", "memory": "4Gi"},
       limits={"cpu": "1000m", "memory": "4Gi"}
    )

    with dag:
        wav_extractor = KubernetesPodOperator(
            namespace=namespace,
            image = image,
            image_pull_policy='Always',
            cmds = [],
            name="task-"+project+"-wav-extractor",
            task_id="task-"+project+"-wav-extractor",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
        )
        voice_separator = KubernetesPodOperator(
            namespace=namespace,
            image = image,
            image_pull_policy='Always',
            cmds = [],
            name="task-"+project+"-voice-separator",
            task_id="task-"+project+"-voice-separator",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
        )
        whisper = KubernetesPodOperator(
            namespace=namespace,
            image = '024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/whisper-cpp:latest',
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = [],
            name="task-"+project+"-whisper",
            task_id="task-"+project+"-whisper",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            resources={
                "request":{"cpu": "1000m", "memory": "4Gi"},
                "limits":{"cpu": "1000m", "memory": "4Gi"}
            },
            is_delete_operator_pod=True,
            get_logs=True,
        )
        llm = KubernetesPodOperator(
            namespace=namespace,
            image = image,
            image_pull_policy='Always',
            cmds = [],
            name="task-"+project+"-llm",
            task_id="task-"+project+"-llm",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
        )
        wav_extractor >> voice_separator >> whisper >> llm

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
