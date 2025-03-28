from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models.param import Param
#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.variable import Variable
from kubernetes.client import models as k8s

namespace = conf.get('kubernetes', 'NAMESPACE') # This will detect the default namespace locally and read the
container_repository = Variable.get("ECR_REPOSITORY")

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

def create_dag(schedule, default_args):
    dag_id = 's3-subtitle-gpu-no-llm'
    project = 'hycu'
    dag = DAG(
        dag_id,
        tags=[project],
        schedule_interval=schedule,
        default_args=default_args,
        is_paused_upon_creation=False,
        params={
            "file": Param("test.mp4", type="string"),
            "collection": Param("finance", type="string"),
        }
    )

    s3_secret = Secret("env",None,"s3")
    volume = k8s.V1Volume(
        name="efs-claim",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="efs-claim"),
    )
    volume_mount = k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/opt/data"
    )
    bash_mount = k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/mnt"
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
        run_id = "{{ run_id }}"
        file = "{{ params.file }}"
        file_prefix = "{{ params.file.rsplit('.', 1)[0] }}"
        collection = "{{ params.collection }}"

        init = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/bash:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["mkdir", "-p", "/mnt/"+run_id],
            name="task-"+project+"-init",
            task_id="task-"+project+"-init",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[volume],
            volume_mounts=[bash_mount]
        )

        prepare =  KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            cmds = ["python", "prepare.py", run_id, collection, file],
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
            image = container_repository+"/hycu/ffmpeg:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["ffmpeg","-i", "/mnt/"+run_id+'/'+file, "-ar", "16000", "/mnt/"+run_id+'/'+file_prefix + ".wav"],
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

        asr = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/auto-subtitle:gpu",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "-m", "auto_subtitle", "/workspace/data/"+ run_id + '/' + file_prefix +".wav"],
            name="task-"+project+"-asr",
            task_id="task-"+project+"-asr",
            annotations={"karpenter.sh/do-not-disrupt": "true"},
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

        upload_srt =  KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "cleanup.py", run_id, collection, file_prefix+"_sync_post.srt", file_prefix+"_sync_post.score"],

            name="task-"+project+"-upload-srt",
            task_id="task-"+project+"-upload-srt",
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

        cleanup = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/bash:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["rm", "-rf", "/mnt/"+run_id],
            name="task-"+project+"-cleanup",
            task_id="task-"+project+"-cleanup",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[volume],
            volume_mounts=[bash_mount]
        )
        init >> prepare >> wav_extractor >> asr >> upload_srt >> cleanup

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


globals()['s3-subtitle-gpu-no-llm'] = create_dag(None, default_args)
