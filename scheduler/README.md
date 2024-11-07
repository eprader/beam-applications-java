## Kubernetes Setup Instructions

Follow the steps below to apply the necessary roles, role bindings, service accounts, and cluster roles in your Kubernetes environment.

Please make sure to create the statefun namespace before

[Mysql deployment] (https://kubernetes.io/docs/tasks/run-application/run-single-instance-stateful-application/)

### Step 1 Deploy mysql database:
```bash
kubectl apply -f https://k8s.io/examples/application/mysql/mysql-pv.yaml
kubectl apply -f https://k8s.io/examples/application/mysql/mysql-deployment.yaml
```

### Step 2: Apply Role and RoleBinding for Default Namespace    
```bash
kubectl apply -f role.yaml
kubectl apply -f rolebinding.yaml
kubectl apply -f service-account.yaml
```

```bash
kubectl apply -f cluster_role.yaml
kubectl apply -f cluster_role_binding.yaml
kubectl apply -f service-account-statefun.yaml
```

