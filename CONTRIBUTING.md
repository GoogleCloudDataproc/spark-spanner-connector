# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Building the project

To build, package, and run all unit tests run the command

```
mvn clean verify
```

### Running Integration tests

To include integration tests when building the project, you need access to
a GCP Project with a valid service account.

For instructions on how to generate a service account and corresponding
credentials JSON see: [Creating a Service Account][1].

Then run the following to build, package, and run all integration tests.
There are two profiles for different test suites:
1. `integration`: for integration tests (`*IntegrationTest.java`)
2. `acceptance`: for acceptance tests (`*AcceptanceTest.java`)

```bash
# To run all integration tests:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account.json
mvn clean verify -Pintegration

# To run all acceptance tests:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account.json
mvn clean verify -Pacceptance
```

#### Integration Test Setup

The integration tests require a Cloud Spanner instance and database. There are two ways to set this up:

1.  **Automatic Test-Managed Database (for CI/temporary use):**
    By default, the test suite will automatically create a temporary database with a unique name for the test run and delete it afterwards. It will also attempt to create a Spanner instance if one is not found. This is convenient for isolated test runs.

2.  **User-Managed Database (for local development):**
    For local development, it's often better to use a stable, long-lived database. The provided `scripts/setup-test-db.sh` script is the recommended way to do this.

    The script will:
    *   Create a Cloud Spanner instance if it doesn't exist.
    *   Create (or drop and recreate) GoogleSQL and PostgreSQL-dialect databases.
    *   Populate the databases with the required schema and test data.

    To use this script, first set the required environment variables, then run it:
    ```bash
    ./scripts/setup-test-db.sh
    ```

    After running the script, you must set an additional environment variable to tell the test suite to use this pre-existing database and not create a temporary one:
    ```bash
    export SPANNER_USE_EXISTING_DATABASE=true
    ```

#### Environment Variables

The integration tests are configured through the following environment variables:

*   `SPANNER_PROJECT_ID`: Your Google Cloud project ID.
*   `SPANNER_INSTANCE_ID`: The ID for the Cloud Spanner instance to use or create.
*   `SPANNER_DATABASE_ID`: The base name for the test databases. The setup script will create a GoogleSQL database with this name and a PostgreSQL database named `${SPANNER_DATABASE_ID}-pg`.
*   `GOOGLE_APPLICATION_CREDENTIALS`: (Optional if you have already authenticated with `gcloud auth application-default login`) Path to your service account credentials JSON file.
*   `SPANNER_USE_EXISTING_DATABASE`: If set to `true`, the tests will use the database specified by `SPANNER_DATABASE_ID` and `SPANNER_INSTANCE_ID` without trying to create or delete it. This is useful for running tests against a database created with `scripts/setup-test-db.sh`.
*   `SPANNER_EMULATOR_HOST`: (Optional) If you are running a local Cloud Spanner emulator, set this to the emulator's address (e.g., `localhost:9010`). Note that the emulator does not support PostgreSQL dialect tests.

#### Running Specific Tests

You can run a single integration test class or a specific test method using Maven.

To run all tests in `Spark31WriteIntegrationTest`, which is in the `spark-3.1-spanner-lib` module:
```bash
mvn verify -Pintegration -Dtest=com.google.cloud.spark.spanner.integration.Spark31WriteIntegrationTest
```

To run a specific method, like `testWriteWithNulls`, within that class:
```bash
mvn verify -Pintegration -Dtest=com.google.cloud.spark.spanner.integration.Spark31WriteIntegrationTest#testWriteWithNulls
```

*Note: Some test classes like `Spark31WriteIntegrationTest` may be subclasses that inherit tests from a base class. You should specify the concrete subclass in the test command.*

The same principles apply to other test classes, modules, and profiles like `-Pacceptance`.

## Code Samples

All code samples must be in compliance with the [java sample formatting guide][3].
Code Samples must be bundled in separate Maven modules.

The samples must be separate from the primary project for a few reasons:
1. Primary projects have a minimum Java version of Java 8 whereas samples can have
   Java version of Java 11. Due to this we need the ability to
   selectively exclude samples from a build run.
2. Many code samples depend on external GCP services and need
   credentials to access the service.
3. Code samples are not released as Maven artifacts and must be excluded from
   release builds.

### Building

```bash
mvn clean verify
```

Some samples require access to GCP services and require a service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account.json
mvn clean verify
```

### Code Formatting

Code in this repo is formatted with
[google-java-format](https://github.com/google/google-java-format).
To run formatting on your project, you can run:
```
mvn com.coveo:fmt-maven-plugin:format
```

[1]: https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account
[2]: https://maven.apache.org/settings.html#Active_Profiles
[3]: https://github.com/GoogleCloudPlatform/java-docs-samples/blob/main/SAMPLE_FORMAT.md
