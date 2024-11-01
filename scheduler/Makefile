.PHONY: configmap image service logs delete

image:
	export $(shell minikube docker-env)
	docker build . -t scheduler-image

service: 
	kubectl create -f config_scheduler/scheduler-manifest.yaml

delete:
	kubectl delete -f config_scheduler/scheduler-manifest.yaml

logs:
	kubectl logs -l app=scheduler