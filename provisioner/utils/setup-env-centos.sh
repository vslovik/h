#!/bin/bash

enable_local_repo=${1:-false}

# This may be crazy, but unless we change this - RHEL will actively
# revert back to localhost.localdomain
sed -ie 's#HOSTNAME=.*$#HOSTNAME='`hostname -f`'#' /etc/sysconfig/network

# Setup rng-tools to improve virtual machine entropy performance.
# The poor entropy performance will cause kerberos provisioning failed.
yum -y install rng-tools
if [ -x /usr/bin/systemctl ] ; then
    sed -i 's@ExecStart=/sbin/rngd -f@ExecStart=/sbin/rngd -f -r /dev/urandom@' /usr/lib/systemd/system/rngd.service
    systemctl daemon-reload
    systemctl start rngd
else
    sed -i.bak 's/EXTRAOPTIONS=\"\"/EXTRAOPTIONS=\"-r \/dev\/urandom\"/' /etc/sysconfig/rngd
    service rngd start
fi

if [ $enable_local_repo == "true" ]; then
    echo "Enabling local yum."
    yum -y install yum-utils
    sudo echo "gpgcheck=0" >> /etc/yum.conf
    sudo yum-config-manager --add-repo file:///matrixnorm-home/output
else
    echo "local yum = $enable_local_repo ; NOT Enabling local yum.  Packages will be pulled from remote..."
fi
