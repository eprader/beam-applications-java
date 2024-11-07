## Kubernetes Setup Instructions

Follow the steps below to apply the necessary roles, role bindings, service accounts, and cluster roles in your Kubernetes environment.

Please make sure to create the statefun namespace before and both metrics_monitors.

[Mysql deployment] (https://kubernetes.io/docs/tasks/run-application/run-single-instance-stateful-application/)

### Step 1 Deploy mysql database:
```bash
kubectl apply -f https://k8s.io/examples/application/mysql/mysql-pv.yaml
kubectl apply -f https://k8s.io/examples/application/mysql/mysql-deployment.yaml
```

### Step 2: Apply Role and RoleBinding for Default Namespace

in the ``config_scheduler``-folder    
```bash
kubectl apply -f role.yaml
kubectl apply -f rolebinding.yaml
kubectl apply -f service-account.yaml

kubectl apply -f kafka-topic-scheduler-input.yaml
kubectl apply -f kafka-topic-statefun-starter-input.yaml

```

in the ``config_frameworks``-folder
```bash
kubectl apply -f service-account-statefun.yaml
```