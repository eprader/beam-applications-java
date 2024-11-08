.PHONY: configmap image_local service logs delete

image_local:
	export $(shell minikube docker-env)
	docker build . -t scheduler-image

service: 
	kubectl create -f config_scheduler/scheduler-manifest.yaml

delete:
	kubectl delete -f config_scheduler/scheduler-manifest.yaml

logs:
	kubectl logs -l app=scheduler -f
#--limit-bytes=500000

docker:
	docker build . -t scheduler
	docker tag scheduler:latest jungerindy/scheduler:latest
	docker push jungerindy/scheduler:latest

