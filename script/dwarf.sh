#!/bin/bash

docker image build -t sidivan/dwarf:$1 -f deployment/dwarf/Dockerfile . --platform=linux/amd64 
docker push sidivan/dwarf:$1