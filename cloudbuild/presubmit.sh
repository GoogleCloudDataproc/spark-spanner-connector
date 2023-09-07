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

readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN install -DskipTests -P3.1
    exit
    ;;

  # Run integration tests with Spanner emulator.
  integrationtest-spanner-emulator)
    # Starts the Spanner emulator and setup the gcloud command.
    gcloud emulators spanner start &
    gcloud config configurations create emulator 
    gcloud config set auth/disable_credentials true
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
    gcloud config configurations activate emulator
    $(gcloud emulators spanner env-init)

    # Sets the env used in the integration test.
    export SPANNER_PROJECT_ID=$PROJECT_ID
    export SPANNER_INSTANCE_ID=test-instance
    export SPANNER_DATABASE_ID=test-database

    
    # TODO: Remove the test filter once Date zone issue is fixed in test SpannerInputPartitionReaderContextTest.
    $MVN test -Dtest=IntegrationTest,SpannerScanBuilderTest,SpannerTableTest,SpannerUtilsTest
    ;;
esac

