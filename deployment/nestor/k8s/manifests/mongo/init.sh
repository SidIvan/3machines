#!/bin/bash

apt-get update -y
apt-get install -y iputils-ping

until ping -c 1 ${HOSTNAME}; do
  sleep 2
done

until /usr/bin/mongosh --eval 'printjson(db.serverStatus())'; do
  sleep 2
done

HOST=nestor-mongodb-0.nestor-mongodb.default.svc.cluster.local:27017

until /usr/bin/mongosh --host=${HOST} --eval 'printjson(db.serverStatus())'; do
  sleep 2
done

if [[ "${HOSTNAME}" != 'nestor-mongodb-0' ]]; then
  until /usr/bin/mongosh --host=${HOST} --eval="printjson(rs.status())" \
        | grep -v "no replset config has been received"; do
    sleep 2
  done
  /usr/bin/mongosh --host=${HOST} \
      --eval="printjson(rs.add('${HOSTNAME}.nestor-mongodb'))"
fi

if [[ "${HOSTNAME}" == 'nestor-mongodb-0' ]]; then
  /usr/bin/mongosh --eval="printjson(rs.initiate(\
      {'_id': 'rs0', 'members': [{'_id': 0, \
        'host': 'nestor-mongodb-0.nestor-mongodb.default.svc.cluster.local:27017'}]}))"
fi