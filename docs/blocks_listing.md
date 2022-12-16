# Blocks Listing
Below is a list of Blocks available for registration in `prefect-gcp`.

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

## Credentials
- [GcpCredentials][prefect_gcp.credentials.GcpCredentials]

To register blocks in this module:
```bash
prefect block register -m prefect_gcp.credentials
```

## Cloud Storage
- [GcsBucket][prefect_gcp.cloud_storage.GcsBucket]

To register blocks in this module:
```bash
prefect block register -m prefect_gcp.cloud_storage
```

## Cloud Run Job
- [CloudRunJob][prefect_gcp.cloud_run.CloudRunJob]

To register blocks in this module:
```bash
prefect block register -m prefect_gcp.cloud_run
```

## AI Platform
- [VertexAICustomTrainingJob][prefect_gcp.aiplatform.VertexAICustomTrainingJob]

To register blocks in this module:
```bash
prefect block register -m prefect_gcp.aiplatform
```
