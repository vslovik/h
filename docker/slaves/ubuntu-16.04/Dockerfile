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
FROM vslovik/puppet:ubuntu-16.04
MAINTAINER Valeriya Slovikovskaya

COPY toolchain /etc/puppet/modules/toolchain

RUN  apt-get clean && apt-get update && puppet apply -e "include toolchain::installer"
COPY . /tmp/vslovik
RUN cd /tmp/vslovik && ./gradlew && cd && rm -rf /tmp/vslovik
