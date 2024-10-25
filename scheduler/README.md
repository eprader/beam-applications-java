## Kubernetes Setup Instructions

Follow the steps below to apply the necessary roles, role bindings, service accounts, and cluster roles in your Kubernetes environment.

Please make sure to create the statefun namespace before
Also copy the FlinkJob.jar into the config_frameworks folder

### Step 1: Apply Role and RoleBinding for Default Namespace

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

