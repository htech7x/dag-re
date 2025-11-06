from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from kubernetes.client import V1Pod, V1ObjectMeta, V1PodSpec, V1Container, V1ResourceRequirements, V1PodSecurityContext, V1VolumeMount, V1Volume, V1EmptyDirVolumeSource


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            labels={
                "enableIdentityHelper": "true"
            }
        ),
        spec=k8s.V1PodSpec(
            security_context=V1PodSecurityContext(
                run_as_user=999,
                run_as_group=999,
                fs_group=99,
                supplemental_groups=[99]
            ),
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "200m", "memory": "1Gi"},
                        limits={"cpu": "2000m", "memory": "4Gi"}
                    ),
                    env=[
                        {"name": "FUNCTIONAL_ACCOUNT", "value": "xxxx"},
                        {"name": "KRB5_CONFIG", "value": "/kcc/krb5.conf"},
                        {"name": "KRB5CCNAME", "value": "FILE:/kcc/Xxx_krb5cache"},
                        {"name": "SIDECAR_ROLE", "value": "KERBEROS"},
                        {"name": "KERBEROS_DONT_RENEW", "value": "true"}
                    ]
                ),
            ]
        )
    )
}

@dag(dag_id="newkrbtest", default_args=default_args, schedule_interval=None, tags=["krb"])
def insert_data_into_test():
    @task(task_id="aifrlow_krb_test1", executor_config=k8s_exec_config_resource_requirements)
    def obtain_connection_insert_to_test():
        print('**********************************dag***********************************')
        print("hello========")
        print('***********************************************************************')
    obtain_connection_insert_to_test()

insert_data_into_test()
