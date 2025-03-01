#!/bin/bash

docker image build -t sidivan/dwarf:$1 -f deployment/dwarf/Dockerfile .
docker push sidivan/dwarf:$1