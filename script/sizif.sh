#!/bin/bash

docker image build -t sidivan/sizif:$1 -f deployment/sizif/Dockerfile .
docker push sidivan/sizif:$1