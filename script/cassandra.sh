#!/bin/bash

docker image build -t sidivan/cassandra:$1 -f k8s/socrates/cassandra/Dockerfile .
docker push sidivan/cassandra:$1