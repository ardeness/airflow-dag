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
    dag_id = 'dubbing'
    project = 'hycu'
    dag = DAG(
        dag_id,
        tags=[project],
        schedule_interval=schedule,
        default_args=default_args,
        is_paused_upon_creation=False,
        params={
            "video_file": Param("test.mp4", type="string"),
            "srt_file": Param("test.srt", type="string"),
            "collection": Param("test", type="string")
        }
    )

    secret_env = Secret("env",None,"lecture-rag")
    s3_secret = Secret("env",None,"s3")
    hudson_secret = Secret("env", None, "hudson")
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
    bash_mount = k8s.V1VolumeMount(
        name="efs-claim",
        mount_path="/mnt"
    )

    with dag:

        run_id = "{{ run_id }}"
        collection = "{{ params.collection }}"
        video_file = "{{ params.video_file }}"
        srt_file = "{{ params.srt_file }}"
        merged_video_file = "{{ params.video_file.rsplit('.', 1)[0] + '_dubbing.' + params.video_file.rsplit('.', 1)[1] }}"

        init = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/bash:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["mkdir", "/mnt/"+run_id],
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
            cmds = ["python", "prepare.py", run_id, collection, video_file, srt_file],
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

        dubbing = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/dubbing:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "dubbing.py", collection+'/'+video_file, "/opt/data/"+run_id+'/'+srt_file],
            name="task-"+project+"-dubbing",
            task_id="task-"+project+"-dubbing",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            secrets = [secret_env, s3_secret, hudson_secret],
            volumes=[volume],
            volume_mounts=[volume_mount]
        )

        merge_audio = KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/ffmpeg:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            #cmds = ["ffmpeg", "-i", "/mnt/"+run_id+'/'+video_file, "$(for f in /mnt/result/*.wav; do echo -n \"-i $f \"; done) -filter_complex \"$(for i in $(seq 1 $(ls /mnt/result/*.wav | wc -l)); do echo -n \"[$i:a:0]\"; done)amix=inputs=$(ls /mnt/result/*.wav | wc -l):duration=longest[a]\" -map 0:v:0 -map \"[a]\" -c:v copy -c:a aac", "/mnt/"+merged_video_file],
            cmds=['/bin/bash', '-c'],
            arguments=[
                f"""
                ffmpeg -i /mnt/{run_id}/{video_file} $(for f in /mnt/{run_id}/result/*.wav; do echo -n "-i $f "; done) \
               -filter_complex "$(for i in $(seq 1 $(ls /mnt/{run_id}/result/*.wav | wc -l)); do echo -n "[$i:a:0]"; done)amix=inputs=$(ls /mnt/{run_id}/result/*.wav | wc -l):duration=longest[a]" \
               -map 0:v:0 -map "[a]" -c:v copy -c:a aac /mnt/{run_id}/{merged_video_file}
                """
            ],
            name="task-"+project+"-merge-audio",
            task_id="task-"+project+"-merge-audio",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            volumes=[volume],
            volume_mounts=[wav_mount]
        )

        upload_dubbing_video =  KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='IfNotPresent',
            cmds = ["python", "cleanup.py", run_id, collection, merged_video_file],
            name="task-"+project+"-upload-dubbing-video",
            task_id="task-"+project+"-upload-dubbing-video",
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

        init >> prepare >> dubbing >> merge_audio >> upload_dubbing_video >> cleanup

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


globals()['dubbing'] = create_dag(None, default_args)
