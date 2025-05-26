# k8s

## Setup local K8s cluster

### Install docker

Follow the guide here: [docker]

### Install kubectl

`kubectl` is the command-line tool for interacting with Kubernetes clusters. You will use it to
manage and inspect your k3d cluster.

Follow the guide here: [kubectl]

### Install k3d

k3d is a lightweight wrapper to run k3s (Rancher Lab's minimal Kubernetes distribution) in docker.

Follow the guide here: [k3d]

## Commands

### Create k8s cluster

```shell
# Start k8s with 1 server and 1 agent, and mount plugins local directory to the cluster
k3d cluster create yscope --servers 1 --agents 1 \
  -v <repo root directory>/pre-built:/fluent-bit/plugins \
  -p 9000:30000@agent:0 \
  -p 9001:30001@agent:0
```

### Deploy minio and log-viewer

```shell
kubectl apply -f minio.yaml
kubectl apply -f yscope-log-viewer-deployment.yaml -f aws-credentials.yaml
kubectl apply -f logs-bucket-creation.yaml -f aws-credentials.yaml
```

### Deploy fluent-bit
#### Fluent-bit-sidecar
```shell 
# Fluent-bit configs are in the yaml file
kubectl apply -f fluent-bit-sidecar.yaml -f fluent-bit-sidecar-config.yaml -f aws-credentials.yaml

# To launch a shell into the fluent-bit container
kubectl exec -it fluent-bit-sidecar -c ubuntu -n default -- /bin/bash

# Test log collection
mkdir -p /logs/$(whoami)/
echo '{"message": "a log message", "level": "error"}' > /logs/$(whoami)/test-0.jsonl

# Inspect the logs for fluent-bit
kubectl logs fluent-bit-sidecar -c fluent-bit-sidecar
# We should get the following
2025/06/11 16:14:29 [info] Uploaded /tmp/clp-irv2-1474549675.clp.zst to s3://logs/root/test-0.log.clp.zst
```

#### Fluent-bit-sidecar-full
```shell 
# Fluent-bit configs are in the yaml file
kubectl apply -f fluent-bit-sidecar-full.yaml -f fluent-bit-sidecar-config-full.yaml -f aws-credentials.yaml

# To launch a shell into the fluent-bit container
kubectl exec -it fluent-bit-sidecar-full -c ubuntu -n default -- /bin/bash

# Test log collection
mkdir -p /logs/$(whoami)/
echo '{"message": "a log message", "level": "error"}' > /logs/$(whoami)/test-1.jsonl

# Inspect the logs for fluent-bit
kubectl logs fluent-bit-sidecar-full -c fluent-bit-sidecar -f
# We should get the following
2025/06/11 16:14:29 [info] Uploaded /tmp/clp-irv2-1474549675.clp.zst to s3://logs/root/test-0.log.clp.zst
```

#### Fluent-bit-daemonset
```shell
# Create fluent-bit service account
kubectl create serviceaccount fluent-bit

# Fluent-bit configs are in the yaml file
kubectl apply -f fluent-bit-daemonset.yaml -f fluent-bit-daemonset-config.yaml -f aws-credentials.yaml -f ubuntu.yaml

# To launch a shell into the fluent-bit container
kubectl exec -it ubuntu -n default -- /bin/bash

# Test log collection
mkdir -p /var/log/$(whoami)/
echo '{"message": "a log message", "level": "error"}' > /var/log/$(whoami)/test-1.jsonl
# Afterwards, /tmp/compressed-logs.clp.zst file should be created containing compressed logs

# port forward
kubectl port-forward minio 9000:9000
```

### Delete cluster

```angular2html
k3d cluster delete yscope
```

[docker]: https://docs.docker.com/engine/install
[k3d]: https://k3d.io/stable/#installation
[kubectl]: https://kubernetes.io/docs/tasks/tools/#kubectl
