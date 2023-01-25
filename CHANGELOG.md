# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- list_folders method for GcsBucket - [#issue](https://github.com/PrefectHQ/prefect-gcp/issues/118)
### Changed

### Deprecated

### Removed

### Fixed

- Listing blobs at the root folder - [#120](https://github.com/PrefectHQ/prefect-gcp/pull/120)

### Security

## 0.2.4

Released on January 20th, 2023.

### Fixed

- Correctly format GCS paths on Windows machines - [#117](https://github.com/PrefectHQ/prefect-gcp/pull/117)

## 0.2.3

Released on January 5th, 2023.

### Fixed

- Wrapping type annotations in quotes to prevent them from loading if the object is not found - [#105](https://github.com/PrefectHQ/prefect-gcp/pull/105)

## 0.2.2

Released on January 3rd, 2023.

### Added

- The `CloudRunJob` timeout parameter is now passed to the GCP TaskSpec. This allows Cloud Run tasks to run for longer than their default of 10min - [#99](https://github.com/PrefectHQ/prefect-gcp/pull/99)

### Fixed

- Improper imports on the top level - [#100](https://github.com/PrefectHQ/prefect-gcp/pull/100)

## 0.2.1

Released on December 23rd, 2022.

### Changed

- Adds handling for ` service_account_info` supplied to `GcpCredentials` as a json formatted string - [#94](https://github.com/PrefectHQ/prefect-gcp/pull/94)

## 0.2.0

Released on December 22nd, 2022.

### Added

- `list_blobs`, `download_object_to_path`, `download_object_to_file_object`, `download_folder_to_path`, `upload_from_path`, `upload_from_file_object`, `upload_from_folder` methods in `GcsBucket` - [#85](https://github.com/PrefectHQ/prefect-gcp/pull/85)
- `GcpSecret` block with `read_secret`, `write_secret`, and `delete_secret` methods - [#86](https://github.com/PrefectHQ/prefect-gcp/pull/86)
- `BigQueryWarehouse` block with `get_connection`, `fetch_one`, `fetch_many`, `fetch_all`, `execute`, `execute_many`, methods - [#88](https://github.com/PrefectHQ/prefect-gcp/pull/88)

### Changed

- Made `GcpCredentials.get_access_token` sync compatible - [#80](https://github.com/PrefectHQ/prefect-gcp/pull/80)
- Breaking: Obfuscated `GcpCredentials.service_account_info` by using `SecretDict` type - [#88](https://github.com/PrefectHQ/prefect-gcp/pull/88)
- `GcsBucket` additionally inherits from `ObjectStorageBlock` - [#85](https://github.com/PrefectHQ/prefect-gcp/pull/85)
- Expose all blocks available in the collection to top level init - [#88](https://github.com/PrefectHQ/prefect-gcp/pull/88)
- Inherit `CredentialsBlock` in `GcpCredentials` - [#92](https://github.com/PrefectHQ/prefect-gcp/pull/92)

### Fixed

- Warning stating `Failed to load collection 'prefect_gcp_aiplatform'` - [#87](https://github.com/PrefectHQ/prefect-gcp/pull/87)

## 0.1.8

Released on December 5th, 2022.

### Added

- `VertexAICustomTrainingJob` infrastructure block - [#75](https://github.com/PrefectHQ/prefect-gcp/pull/75)

## 0.1.7

Released on December 2nd, 2022.

### Added

- `CloudJobRun.kill` method for cancellation support - [#76](https://github.com/PrefectHQ/prefect-gcp/pull/76)

## 0.1.6

Released on October 7th, 2022.

### Fixed

- Validation errors for CPU and Memory being raised incorrectly - [#64](https://github.com/PrefectHQ/prefect-gcp/pull/64)

## 0.1.5

Released on September 28th, 2022.

### Changed

- Invoke `google.auth.default` if both `service_account_info` and `service_account_file` is not specified - [#57](https://github.com/PrefectHQ/prefect-gcp/pull/57)

### Fixed

- Retrieving the `project_id` from service account or `quota_project_id` from gcloud CLI if `project` is not specified - [#57](https://github.com/PrefectHQ/prefect-gcp/pull/57)

## 0.1.4

Released on September 19th, 2022.

### Added
- `CloudRunJob` infrastructure block - [#48](https://github.com/PrefectHQ/prefect-gcp/pull/48)
- `GcsBucket` block - [#41](https://github.com/PrefectHQ/prefect-gcp/pull/41)
- `external_config` keyword argument in `bigquery_create_table` task - [#53](https://github.com/PrefectHQ/prefect-gcp/pull/53)
- `content_type` keyword argument in `cloud_storage_upload_blob_from_file` task - [#47](https://github.com/PrefectHQ/prefect-gcp/pull/47)
- `**kwargs` for all tasks in the module `cloud_storage.py` - [#47](https://github.com/PrefectHQ/prefect-gcp/pull/47)

### Changed
- Made `schema` keyword argument optional in `bigquery_create_table` task, thus the position of the keyword changed - [#53](https://github.com/PrefectHQ/prefect-gcp/pull/53)
- Allowed `~` character to be used in the path for service account file - [#38](https://github.com/PrefectHQ/prefect-gcp/pull/38)

### Fixed

- `ValidationError` using `GcpCredentials.service_account_info` in `prefect-dbt` - [#44](https://github.com/PrefectHQ/prefect-gcp/pull/44)

## 0.1.3
Released on July 22nd, 2022.

### Added
- Added setup.py entry point - [#35](https://github.com/PrefectHQ/prefect-gcp/pull/35)

## 0.1.2
Released on July 22nd, 2022.

### Changed
- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#30](https://github.com/PrefectHQ/prefect-gcp/pull/30)
- Converted GcpCredentials into a Block - [#31](https://github.com/PrefectHQ/prefect-gcp/pull/31).

## 0.1.1

Released on July 11th, 2022

### Changed

- Improved error handle and instruction for extras - [#18](https://github.com/PrefectHQ/prefect-gcp/pull/18)

## 0.1.0

Released on March 17th, 2022.

### Added

- `cloud_storage_copy_blob`, `cloud_storage_create_bucket`, `cloud_storage_download_blob_as_bytes`, `cloud_storage_download_blob_to_file`, `cloud_storage_upload_blob_from_file`, and `cloud_storage_upload_blob_from_string` tasks - [#1](https://github.com/PrefectHQ/prefect-gcp/pull/1)
- `bigquery_create_table`, `bigquery_insert_stream`, `bigquery_load_cloud_storage`, `bigquery_load_file`, and `bigquery_query`, tasks - [#2](https://github.com/PrefectHQ/prefect-gcp/pull/2)
- `create_secret`, `delete_secret`, `delete_secret_version`, `read_secret`, and `update_secret` tasks - [#3](https://github.com/PrefectHQ/prefect-gcp/pull/5)
