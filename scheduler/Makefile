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

build:
	docker build . -t scheduler

tag:
	docker tag scheduler:latest jungerindy/scheduler:latest

push:	
	docker push jungerindy/scheduler:latest

docker:
	make build tag push

