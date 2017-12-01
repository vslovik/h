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

PUPPET_DIR=../../deploy/puppet

if [ -e .provision_id ]; then
    PROVISION_ID=`cat .provision_id`
fi
if [ -n "$PROVISION_ID" ]; then
    NODES=(`docker-compose -p $PROVISION_ID ps -q`)
fi

log() {
    echo -e "\n[LOG] $1\n"
}

env-check() {
    echo "Environment check..."
    echo "Check docker:"
    docker -v || exit 1
    echo "Check docker-compose:"
    docker-compose -v || exit 1
    echo "Check ruby:"
    ruby -v || exit 1
}

create() {
    if [ -e .provision_id ]; then
        log "Cluster already exist! Run ./$PROG -d to destroy the cluster or delete .provision_id file and containers manually."
        exit 1;
    fi
    echo "`date +'%Y%m%d_%H%M%S'`_R$RANDOM" > .provision_id
    PROVISION_ID=`cat .provision_id`
    # Create a shared /etc/hosts and hiera.yaml that will be both mounted to each container soon
    mkdir -p config/hieradata 2> /dev/null
    echo > ./config/hiera.yaml
    echo > ./config/hosts
    export DOCKER_IMAGE=$(get-yaml-config docker image)
    export MEM_LIMIT=$(get-yaml-config docker memory_limit)

    # Startup instances
    docker-compose -p $PROVISION_ID up -d --scale matrixnorm=$1 --no-recreate
    if [ $? -ne 0 ]; then
        log "Docker container(s) startup failed!";
        exit 1;
    fi

    # Get the headnode FQDN
    NODES=(`docker-compose -p $PROVISION_ID ps -q`)
    hadoop_head_node=`docker inspect --format {{.Config.Hostname}}.{{.Config.Domainname}} ${NODES[0]}`

    echo "hadoop head node: $hadoop_head_node"

    # Fetch configurations form specificed yaml config file
    repo=$(get-yaml-config repo)
    components="[`echo $(get-yaml-config components) | sed 's/ /, /g'`]"
    distro=$(get-yaml-config distro)
    enable_local_repo=$(get-yaml-config enable_local_repo)
    generate-config "$hadoop_head_node" "$repo" "$components"

    # Start provisioning
    generate-hosts
    bootstrap $distro $enable_local_repo
}

destroy() {
    docker exec ${NODES[0]} bash -c "umount /etc/hosts; rm -f /etc/hosts"
    if [ -n "$PROVISION_ID" ]; then
        docker-compose -p $PROVISION_ID stop
        docker-compose -p $PROVISION_ID rm -f
    fi
    rm -rvf ./config .provision_id
}

# cat config.yaml | ruby -ryaml -e "data = YAML::load(STDIN.read); puts data['docker']['memory_limit'];" | tr -d '\r'
# cat config.yaml | ruby -ryaml -e "data = YAML::load(STDIN.read); puts data['docker']['image'];" | tr -d '\r'
get-yaml-config() {
    RUBY_EXE=ruby
    if [ $# -eq 1 ]; then
        RUBY_SCRIPT="data = YAML::load(STDIN.read); puts data['$1'];"
    elif [ $# -eq 2 ]; then
        RUBY_SCRIPT="data = YAML::load(STDIN.read); puts data['$1']['$2'];"
    else
        echo "The yaml config retrieval function can only take 1 or 2 parameters.";
        exit 1;
    fi
    cat ${yamlconf} | $RUBY_EXE -ryaml -e "$RUBY_SCRIPT" | tr -d '\r'
}

generate-config() {
    log "Matrixnorm Puppet configurations are shared between instances, and can be modified under config/hieradata"
    cat $PUPPET_DIR/hiera.yaml >> ./config/hiera.yaml
    cp -vfr $PUPPET_DIR/hieradata ./config/
    cat > ./config/hieradata/site.yaml << EOF
bigtop::hadoop_head_node: $1
hadoop::hadoop_storage_dirs: [/data/1, /data/2]
bigtop::bigtop_repo_uri: $2
hadoop_cluster_node::cluster_components: $3
EOF
}

generate-hosts() {
    for node in ${NODES[*]}; do
        echo $node
        entry=`docker inspect --format "{{.NetworkSettings.IPAddress}} {{.Config.Hostname}}.{{.Config.Domainname}} {{.Config.Hostname}}" $node`
        docker exec ${NODES[0]} bash -c "echo $entry >> /etc/hosts"
    done
    wait
    # This must be the last entry in the /etc/hosts
    docker exec ${NODES[0]} bash -c "echo '127.0.0.1 localhost' >> ./etc/hosts"
}

bootstrap() {
    for node in ${NODES[*]}; do
        docker exec $node bash -c "/matrixnorm-home/provisioner/utils/setup-env-$1.sh $2" &
    done
    wait
}

provision() {
    for node in ${NODES[*]}; do
        matrixnorm-puppet $node &
    done
    wait
}

matrixnorm-puppet() {
    docker exec $1 bash -c 'puppet apply --parser future --modulepath=/matrixnorm-home/deploy/puppet/modules:/etc/puppet/modules /matrixnorm-home/deploy/puppet/manifests'
}

copy-to-instances() {
    for node in ${NODES[*]}; do
        docker cp  $1 $node:$2 &
    done
    wait
}

list() {
    local msg
    msg=$(docker-compose -p $PROVISION_ID ps 2>&1)
    if [ $? -ne 0 ]; then
        msg="Cluster hasn't been created yet."
    fi
    echo "$msg"
}

while [ $# -gt 0 ]; do
    case "$1" in
    -c|--create)
        if [ $# -lt 2 ]; then
          echo "Create requires a number" 1>&2
          usage
        fi
        env-check
        create $2
        shift 2;;
    -E|--env-check)
        env-check
        shift;;
    -p|--provision)
        provision
        shift;;
    -h|--help)
        usage
        shift;;
    -d|--destroy)
        destroy
        shift;;
    -l|--list)
        list
        shift;;
    *)
        echo "Unknown argument: '$1'" 1>&2
        usage;;
    esac
done