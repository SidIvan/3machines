#!/bin/bash

docker image build -t sidivan/nestor:$1 -f deployment/nestor/Dockerfile . --platform=linux/amd64 
docker push sidivan/nestor:$1