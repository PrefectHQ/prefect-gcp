# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- `external_config` keyword argument in `bigquery_create_table` task - [#53](https://github.com/PrefectHQ/prefect-gcp/pull/53)
- `content_type` keyword argument in `cloud_storage_upload_blob_from_file` task - [#47](https://github.com/PrefectHQ/prefect-gcp/pull/47)
- `**kwargs` for all tasks in the module `cloud_storage.py` - [#47](https://github.com/PrefectHQ/prefect-gcp/pull/47)

### Changed
- Made `schema` keyword argument optional in `bigquery_create_table` task, thus the position of the keyword changed - [#53](https://github.com/PrefectHQ/prefect-gcp/pull/53)
- Allowed `~` character to be used in the path for service account file - [#38](https://github.com/PrefectHQ/prefect-gcp/pull/38)

### Deprecated

### Removed

### Fixed

- `ValidationError` using `GcpCredentials.service_account_info` in `prefect-dbt` - [#44](https://github.com/PrefectHQ/prefect-gcp/pull/44)

### Security

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
