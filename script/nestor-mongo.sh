#!/bin/bash

docker image build -t sidivan/nestor-mongo:$1 -f deployment/nestor/k8s/manifests/mongo/Dockerfile .
docker push sidivan/nestor-mongo:$1