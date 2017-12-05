# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM vslovik/deploy:ubuntu-16.04
MAINTAINER vslovik@gmail.com

WORKDIR /tmp/vslovik
COPY deploy/puppet deploy/puppet
COPY deploy/puppet/hieradata /etc/puppet/hieradata
RUN  cp deploy/puppet/hiera.yaml /etc/puppet
COPY docker/pseudo-cluster/config pseudo-cluster
RUN cp -r pseudo-cluster/* /etc/puppet

RUN puppet apply -d --modulepath=/tmp/vslovik/deploy/puppet/modules:/etc/puppet/modules /tmp/vslovik/deploy/puppet/manifests/site.pp

RUN apt-get -y install hadoop-hdfs-namenode hadoop-yarn-resourcemanager \
 hadoop-doc hadoop-client hadoop-yarn-proxyserver \
 hadoop-mapreduce-historyserver libhdfs0-dev hadoop-hdfs-fuse

ENTRYPOINT ["/tmp/vslovik/pseudo-cluster/configure.sh"]
