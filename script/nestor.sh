#!/bin/bash

docker image build -t sidivan/nestor:$1 -f deployment/nestor/Dockerfile .
docker push sidivan/nestor:$1