from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models.param import Param
#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
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
    dag_id = 'hycu-subtitle-gpu'
    project = 'hycu'
    dag = DAG(
        dag_id,
        tags=[project],
        schedule_interval=schedule,
        default_args=default_args,
        is_paused_upon_creation=False,
        params={
            "file": Param("test.mp4", type="string"),
            # "file_prefix": Param("test", type="string"),
            "collection": Param("finance", type="string"),
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
    wav_mount =  k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/mnt"
    )
    gpu_mount = k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/workspace/data"
    )
    asr_compute_resources = k8s.V1ResourceRequirements(
       requests={"nvidia.com/gpu": "1", "memory": "50Gi"},
       limits={"nvidia.com/gpu": "1", "memory": "50Gi"}
    )
    gpu_toleration = k8s.V1Toleration(
        key= "nvidia.com/gpu",
        operator="Exists",
        effect="NoSchedule"
    )

    with dag:

        file = "{{ params.file }}"
        collection = "{{ params.collection }}"
        # file_prefix = "{{ params.file_prefix }}"
        # file_prefix = {{ params.file_prefix.rsplit('.', 1)[1] }}
        file_prefix = "{{ params.file.rsplit('.', 1)[0] }}"

        prepare =  KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
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

        wav_extractor = KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/ffmpeg:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            cmds = ["ffmpeg","-i", "/mnt/"+file, "-ar", "16000", "/mnt/"+ file_prefix + ".wav"],
            name="task-"+project+"-wav-extractor",
            task_id="task-"+project+"-wav-extractor",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[volume],
            volume_mounts=[wav_mount]
        )

        # voice_separator = KubernetesPodOperator(
        #     namespace=namespace,
        #     image = image,
        #     image_pull_policy='IfNotPresent',
        #     cmds = [],
        #     name="task-"+project+"-voice-separator",
        #     task_id="task-"+project+"-voice-separator",
        #     in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        #     cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
        #     config_file=config_file,
        #     #resources=compute_resources,
        #     is_delete_operator_pod=True,
        #     get_logs=True,
        # )

        asr = KubernetesPodOperator(
            namespace=namespace,
            image = '024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/auto-subtitle:latest',
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            cmds = ["python", "-m", "auto_subtitle", "/workspace/data/"+ file_prefix +".wav"],
            name="task-"+project+"-asr",
            task_id="task-"+project+"-asr",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            container_resources=asr_compute_resources,
            tolerations=[gpu_toleration],
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[volume],
            volume_mounts=[gpu_mount],
            startup_timeout_seconds=3600
        )
        srt_correction =  KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/lecture-rag:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "correction.py", "/opt/data/"+file_prefix+"_sync_post", collection],
            name="task-"+project+"-srt-correction",
            task_id="task-"+project+"-srt-correction",
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
        cleanup =  KubernetesPodOperator(
            namespace=namespace,
            image = "024848470331.dkr.ecr.ap-northeast-2.amazonaws.com/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            cmds = ["python", "cleanup.py", file_prefix+"_sync_post_rag.srt", file_prefix+"_sync_post.srt", file_prefix+"_sync_post.score", file_prefix+"_sync_post_rag.score"],
            name="task-"+project+"-cleanup",
            task_id="task-"+project+"-cleanup",
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
        prepare >> wav_extractor >> asr >> srt_correction >> cleanup

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


globals()['hycu-subtitle-gpu'] = create_dag(None, default_args)
