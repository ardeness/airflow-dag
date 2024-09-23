from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
#from airflow.providers.mongo.hooks.mongo import MongoHook
from kubernetes.client import models as k8s

mongo_conn_id = 'mongodb_conn'

namespace = conf.get('kubernetes', 'NAMESPACE') # This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.

if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

#def create_dag(dag_id, project, schedule, default_args):
def create_dag(meta, schedule, default_args):
    dag_id = meta['_id']
    project = meta['project_id']
    # name = meta['name']
    image = meta['image']
    cmd = meta['cmd']
    dag = DAG(dag_id, tags=[project], schedule_interval=schedule, default_args=default_args, is_paused_upon_creation=False)

    compute_resources = k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "2Gi"},
        limits={"cpu": "500m", "memory": "2Gi"}
    )

    # s3_access_key = Secret(
    #     deploy_type="env",
    #     deploy_target="S3_ACCESS_KEY",
    #     secret="s3-secret",
    #     key="S3_ACCESS_KEY"
    # )
    # s3_secret_key = Secret(
    #     deploy_type="env",
    #     deploy_target="S3_SECRET_KEY",
    #     secret="s3-secret",
    #     key="S3_SECRET_KEY"
    # )
    # s3_endpoint = Secret(
    #     deploy_type="env",
    #     deploy_target="S3_ENDPOINT",
    #     secret="s3-secret",
    #     key="S3_ENDPOINT"
    # )

    env={
        "LTDB_INGEST_URL": "http://ltdb-http-svc:8080/ingest",
        "LTDB_QUERY_URL": "http://ltdb-http-svc:8080/query",
        "ID_GENERATOR_HOST": "id-generator-1.id-generator",
        "ID_GENERATOR_PORT": "6379"
    }

    with dag:
        KubernetesPodOperator(
            namespace=namespace,
            #image="harbor.k8s.lightningdb/metavision/metavision1_pipeline_container:latest",
            image = image,
            image_pull_policy='Always',
            # secrets=[s3_access_key, s3_secret_key, s3_endpoint],
            # env_vars=env,
            #cmds=['python', 'app.py', '{{dag_run.conf["bucket"]}}', '{{dag_run.conf["key"]}}'],
            cmds = [] if cmd == None else cmd.split(' '),
            #labels={"foo": "bar"},
            #labels={"sidecar.istio.io/inject":"true","type":"workflow"},
            #annotations={'sidecar.istio.io/userVolume': '[{"name":"outbound-filter","configMap":{"name":"outbound-filter"}}]', "sidecar.istio.io/userVolumeMount": '[{"mountPath":"/var/local/outbound","name":"outbound-filter"}]'},
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

#results = MongoHook(mongo_conn_id).get_collection(mongo_db='workflow', mongo_collection='workflow').find()
#for meta in results:
meta = [
    { '_id': 0, 'project_id': 'test0', 'image': 'hello-world'},
    { '_id': 1, 'project_id': 'test1', 'image': 'hello-world'}
]
for meta in results:
    default_args = {
        'owner': meta['project_id'],
        'depends_on_past': False,
        'start_date': datetime(2019, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }

    globals()[meta['_id']] = create_dag(meta, None, default_args)
