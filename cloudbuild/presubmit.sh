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

    $MVN install -DskipTests -P3.1,3.2,3.3,3.5,4.0,4.1
    exit
    ;;

  # Run unit tests
  unittest)
    $MVN test -T 1C -P3.1,3.2,3.3,3.5,4.0,4.1
    ;;


  # Run integration tests with Spanner emulator.
  integrationtest-spanner-emulator)
    # Starts the Spanner emulator and setup the gcloud command.
    # Sets the env used in the integration test.
    echo "Starting Spanner emulator..."
    export TZDIR=/usr/share/zoneinfo
    gcloud emulators spanner start --host-port=0.0.0.0:9010 &
    SP_EMU_PID=$!
    trap 'kill $SP_EMU_PID 2>/dev/null || true' EXIT

    # Wait for the emulator to start up and listen on port 9010
    timeout 15 bash -c 'until echo > /dev/tcp/localhost/9010; do sleep 0.5; done' 2>/dev/null || sleep 5

    $MVN -P3.1,3.2,3.3,3.5,4.0,4.1,integration clean verify -Dsurefire.skip=true -Dintegration.forkCount=1
    ;;

  acceptance-test)
    $MVN -P3.1,3.2,3.3,3.5,4.0,4.1,acceptance failsafe:integration-test failsafe:verify
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac
