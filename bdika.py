from kubernetes import client, config
from kubernetes.stream import stream


def list_pods_and_env_vars():
    # Load kubeconfig and initialize client
    config.load_kube_config()
    v1 = client.CoreV1Api()

    # Get all namespaces
    namespaces = v1.list_namespace()

    for ns in namespaces.items:
        namespace = ns.metadata.name
        print(f"Namespace: {namespace}")

        # Get all pods in the current namespace
        pods = v1.list_namespaced_pod(namespace)

        for pod in pods.items:
            pod_name = pod.metadata.name
            print(f"  Pod: {pod_name}")

            # Get containers in the current pod
            containers = pod.spec.containers

            for container in containers:
                container_name = container.name
                print(f"    Container: {container_name}")

                # Exec command to get environment variables
                exec_command = [
                    '/bin/sh',
                    '-c',
                    'env'
                ]

                try:
                    resp = stream(
                        v1.connect_get_namespaced_pod_exec,
                        pod_name,
                        namespace,
                        container=container_name,
                        command=exec_command,
                        stderr=True,
                        stdin=False,
                        stdout=True,
                        tty=False
                    )
                    print(
                        f"    Environment variables for pod {pod_name} in namespace {namespace}, container {container_name}:\n{resp}")
                except client.rest.ApiException as e:
                    print(
                        f"Exception when calling CoreV1Api->connect_get_namespaced_pod_exec: {e}\n")
                print("------------------------------------------------------------")


# Run the function
list_pods_and_env_vars()
