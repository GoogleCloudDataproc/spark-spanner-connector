#!/bin/bash
# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

readonly MVN_NT="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly MVN="${MVN_NT} -t toolchains.xml"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN_NT toolchains:generate-jdk-toolchains-xml -Dtoolchain.file=toolchains.xml
    cat toolchains.xml

    $MVN install -DskipTests -P3.1,3.2,3.3,3.5,4.0
    exit
    ;;

  # Run unit tests
  unittest)
    $MVN test -T 1C -P3.1,3.2,3.3,3.5,4.0
    ;;


  # Run integration tests with Spanner Omni.
  integrationtest-real-spanner)
    echo "Starting Spanner Omni..."
    export JAVA_HOME=/omni-jre
    export PATH=/omni-jre/bin:$PATH
    /google/spanner/bin/spanner start-single-server > /workspace/spanner-omni.log 2>&1 &
    sleep 5
    /app/bin/spanner-console > /workspace/spanner-console.log 2>&1 &
    sleep 10

    echo "Restoring default Java runtime env for Maven..."
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
    export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

    export SPANNER_OMNI_ENDPOINT=localhost:15000
    export SPANNER_DATABASE_ID=testdb

    $MVN -P3.1,3.2,3.3,3.5,integration failsafe:integration-test failsafe:verify -Dintegration.forkCount=1
    ;;



  acceptance-test)
    $MVN -P3.1,3.2,3.3,3.5,acceptance failsafe:integration-test failsafe:verify
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
