#!/bin/bash

enable_local_repo=${1:-false}

# Setup rng-tools to improve virtual machine entropy performance.
# The poor entropy performance will cause kerberos provisioning failed.
apt-get -y install rng-tools
sed -i.bak 's@#HRNGDEVICE=/dev/null@HRNGDEVICE=/dev/urandom@' /etc/default/rng-tools
service rng-tools start

if [ $enable_local_repo == "true" ]; then
    echo "deb file:///matrixnorm-home/output/apt bigtop contrib" > /etc/apt/sources.list.d/bigtop-home_output.list
    apt-get update
else
    apt-get install -y apt-transport-https
    echo "local apt = $enable_local_repo ; NOT Enabling local apt. Packages will be pulled from remote..."
fi