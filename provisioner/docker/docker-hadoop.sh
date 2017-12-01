#!/bin/bash

usage() {
    echo "usage: $PROG [-C file ] args"
    echo "       -C file                                   Use alternate file for config.yaml"
    echo "  commands:"
    echo "       -c NUM_INSTANCES, --create NUM_INSTANCES  Create a Docker based Hadoop cluster"
    echo "       -d, --destroy                             Destroy the cluster"
    echo "       -e, --exec INSTANCE_NO|INSTANCE_NAME      Execute command on a specific instance. Instance can be specified by name or number."
    echo "                                                 For example: $PROG --exec 1 bash"
    echo "                                                              $PROG --exec docker_1 bash"
    echo "       -E, --env-check                           Check whether required tools has been installed"
    echo "       -l, --list                                List out container status for the cluster"
    echo "       -p, --provision                           Deploy configuration changes"
    echo "       -h, --help"
    exit 1
}

PROG=`basename $0`

if [ $# -eq 0 ]; then
    usage
fi

yamlconf="config.yaml"

if [ -e .provision_id ]; then
    PROVISION_ID=`cat .provision_id`
fi
if [ -n "$PROVISION_ID" ]; then
    NODES=(`docker-compose -p $PROVISION_ID ps -q`)
fi

env-check() {
    echo "Environment check..."
    echo "Check docker:"
    docker -v || exit 1
    echo "Check docker-compose:"
    docker-compose -v || exit 1
    echo "Check ruby:"
    ruby -v || exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
    -E|--env-check)
        env-check
        shift;;
    -h|--help)
        usage
        shift;;
    *)
        echo "Unknown argument: '$1'" 1>&2
        usage;;
    esac
done