from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models.param import Param
#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.variable import Variable
from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification
from kubernetes.client import models as k8s

namespace = conf.get('kubernetes_executor', 'NAMESPACE') # This will detect the default namespace locally and read the
container_repository = Variable.get("ECR_REPOSITORY")

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

def create_dag(schedule, default_args):
    dag_id = 'srt_correction'
    project = 'hycu'
    dag = DAG(
        dag_id,
        tags=[project],
        schedule_interval=schedule,
        default_args=default_args,
        is_paused_upon_creation=False,
        params={
            "curriName": Param("기술경영과전략|2주차|OT", type="string"),
            "term": Param("202110", type="string"),
            "curriCode": Param("41XDA", type="string"),
            "week": Param("2", type="string"),
            "week_seq": Param("00", type="string"),
            "resultFileName": Param("CT_V000000010002.json", type="string"),
            "metadata": Param("key1:value1, key2:value2", type=["null", "string"]),
        }
        # on_success_callback=[
        #     send_slack_webhook_notification(
        #         slack_webhook_conn_id="slackwebhook", text="The dag {{ dag.dag_id }} success"
        #     )
        # ],
        # on_failure_callback=[
        #     send_slack_webhook_notification(
        #         slack_webhook_conn_id="slackwebhook", text="The dag {{ dag.dag_id }} failed"
        #     )
        # ],
    )

    lecture_secret = Secret("env",None,"lecture-rag")
    s3_secret = Secret("env",None,"s3")
    cms_ftp_secret = Secret("env", None, "cms-ftp")
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

    with dag:
        run_id = "{{ run_id }}"
        curriCode = "{{ params.curriCode }}"
        file_prefix = "{{ params.curriCode + '_' + params.term + '_' + params.week + '_' + params.week_seq }}"
        metadata = " {{ params.metadata.replace(' ', '') if params.metadata else ''}}"
        resultFileName = " {{ params.resultFileName }}"

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
            cmds = ["python", "prepare.py", run_id, curriCode, file_prefix+".srt", file_prefix+".score"],
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

        srt_correction =  KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/lecture-rag:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "correction.py", "/opt/data/"+run_id+'/'+file_prefix, curriCode, metadata],
            name="task-"+project+"-srt-correction",
            task_id="task-"+project+"-srt-correction",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            secrets = [lecture_secret],
            volumes=[volume],
            volume_mounts=[volume_mount]
        )

        upload_srt =  KubernetesPodOperator(
            namespace=namespace,
            image = container_repository+"/hycu/setup:latest",
            image_pull_secrets=[k8s.V1LocalObjectReference("ecr")],
            image_pull_policy='Always',
            cmds = ["python", "hycu_cms_cleanup.py", run_id, curriCode, file_prefix+"_sync_post_rag.score", resultFileName, file_prefix+".wav", file_prefix+"_sync_post.srt", file_prefix+"_sync_post.score", file_prefix+"_sync_post_rag.srt", file_prefix+"_sync_post_rag.score"],
            name="task-"+project+"-upload-srt",
            task_id="task-"+project+"-upload-srt",
            in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
            cluster_context="docker-for-desktop",  # is ignored when in_cluster is set to True
            config_file=config_file,
            #resources=compute_resources,
            is_delete_operator_pod=True,
            get_logs=True,
            secrets = [s3_secret, cms_ftp_secret],
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

        init >> prepare >> srt_correction >> upload_srt >> cleanup

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


globals()['srt_correction'] = create_dag(None, default_args)
