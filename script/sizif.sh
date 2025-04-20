#!/bin/bash

docker image build -t sidivan/sizif:$1 -f deployment/sizif/Dockerfile . --platform=linux/amd64 
docker push sidivan/sizif:$1