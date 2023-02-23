# Coordinate and use GCP in your dataflow with `prefect-gcp`

<p align="center">
    <img src="https://user-images.githubusercontent.com/15331990/214915616-6369697e-bc84-400a-a584-845d795a68f2.png">
    <br>
    <a href="https://pypi.python.org/pypi/prefect-gcp/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-gcp?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-gcp/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-gcp/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-gcp?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

The `prefect-gcp` collection makes it easy to leverage the capabilities of Google Cloud Platform (GCP) in your flows, featuring support for Vertex AI, Cloud Run, BigQuery, Cloud Storage, and Secret Manager.

Visit the full docs [here](https://PrefectHQ.github.io/prefect-gcp).

### Installation

To start using `prefect-gcp`:

```bash
pip install prefect-gcp
```

To install extras, see [here](https://prefecthq.github.io/prefect-gcp/#installation).

### Feedback

If you encounter any bugs while using `prefect-gcp`, feel free to open an issue in the [`prefect-gcp`](https://github.com/PrefectHQ/prefect-gcp) repository.

If you have any questions or issues while using `prefect-gcp`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-gcp`](https://github.com/PrefectHQ/prefect-gcp) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-gcp`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-gcp/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
