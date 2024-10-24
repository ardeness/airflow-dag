from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

namespace = conf.get('kubernetes', 'NAMESPACE') # This will detect the default namespace locally and read the

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

def create_dag(schedule, default_args):
    dag_id = 'import_lecture_embedding'
    project = 'hycu'
    dag = DAG(
        dag_id,
        tags=[project],
        schedule_interval=schedule,
        default_args=default_args,
        is_paused_upon_creation=False,
        params={
            "file": Param("13", type="string"),
            "collection": Param("test", type="string"),
        }
    )
    secret_env = Secret("env",None,"lecture-rag")
    s3_secret = Secret("env",None,"s3")

    volume = k8s.V1Volume(
        name="efs-claim",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="efs-claim"),
    )
    volume_mount = k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/opt/data"
    )
    with dag:
        file = "{{ params.file}}"
        collection = "{{ params.collection}}"
        prepare =  KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/setup:latest",
            image_pull_policy='Always',
            cmds = ["python", "prepare.py", file],
            name="task-"+project+"-prepare",
            task_id="task-"+project+"-prepare",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            secrets = [s3_secret],
            volumes=[volume],
            volume_mounts=[volume_mount]
        )

        import_lecture_embedding =  KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/lecture-rag:latest",
            image_pull_policy='Always',
            cmds = ["python", "embedding_extract.py", "/opt/data/"+file, collection],
            name="task-"+project+"-import-lecture-embedding",
            task_id="task-"+project+"-import-lecture-embedding",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            secrets = [secret_env],
            volumes=[volume],
            volume_mounts=[volume_mount]
        )
        prepare >> import_lecture_embedding

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


globals()['import_lecture_embedding'] = create_dag(None, default_args)
