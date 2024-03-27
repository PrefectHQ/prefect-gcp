from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.firestore_v1 import DocumentReference
from prefect import Flow

from prefect_gcp.firestore import (
    firestore_create_collection,
    firestore_create_document,
    firestore_delete_document,
    firestore_query_collection,
    firestore_read_collection,
    firestore_read_document,
    firestore_update_document,
)


@pytest.fixture
def gcp_credentials_mock():
    mock_credentials = MagicMock()
    mock_firestore_client = MagicMock()
    mock_credentials.get_firestore_client.return_value = mock_firestore_client
    return mock_credentials


@pytest.fixture
def document_reference_mock():
    return MagicMock(spec=DocumentReference)


def test_firestore_create_collection_success(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_create_collection"
    ) as mock_create_collection:
        mock_create_collection.return_value = MagicMock(spec=DocumentReference)

        @Flow
        def test_flow():
            collection_ref = firestore_create_collection(
                collection="users",
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            assert isinstance(collection_ref, DocumentReference)

        test_flow._run()


def test_firestore_create_document_success(
    gcp_credentials_mock, document_reference_mock
):
    with patch(
        "prefect_gcp.firestore.firestore_create_document"
    ) as mock_create_document:
        mock_create_document.return_value = document_reference_mock

        @Flow
        def test_flow():
            document_data = {"name": "John", "age": 30}
            document_ref = firestore_create_document(
                collection="users",
                document_data=document_data,
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            assert isinstance(document_ref, DocumentReference)

        test_flow._run()


def test_firestore_read_collection_success(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_read_collection"
    ) as mock_read_collection:
        mock_read_collection.return_value = [{"name": "John", "age": 30}]

        @Flow
        def test_flow():
            documents = firestore_read_collection(
                collection="users",
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            assert isinstance(documents, list)
            assert documents[0]["name"] == "John"

        test_flow._run()


def test_firestore_update_document_success(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_update_document"
    ) as mock_update_document:
        mock_update_document.return_value = {"name": "John", "age": 35}

        @Flow
        def test_flow():
            update_data = {"age": 35}
            updated_doc = firestore_update_document(
                collection="users",
                document_id="document_id",
                update_data=update_data,
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            assert updated_doc["age"] == 35

        test_flow._run()


def test_firestore_delete_document_success(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_delete_document"
    ) as mock_delete_document:

        @Flow
        def test_flow():
            firestore_delete_document(
                collection="users",
                document_id="document_id",
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            mock_delete_document.assert_called_once()

        test_flow._run()


def test_firestore_read_document_success(gcp_credentials_mock):
    with patch("prefect_gcp.firestore.firestore_read_document") as mock_read_document:
        mock_read_document.return_value = {"name": "John", "age": 30}

        @Flow
        def test_flow():
            document = firestore_read_document(
                collection="users",
                document_id="document_id",
                gcp_credentials=gcp_credentials_mock,
                project="my-project",
                location="us-central1",
            )
            assert document["name"] == "John"

        test_flow._run()


def test_firestore_query_collection_success(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_query_collection"
    ) as mock_query_collection:
        mock_query_collection.return_value = [{"id": "123", "name": "John", "age": 30}]

        @Flow
        def test_flow():
            documents = firestore_query_collection(
                collection="users",
                gcp_credentials=gcp_credentials_mock,
                query_key="age",
                query_value=30,
                return_whole_document=True,
                project="my-project",
                location="us-central1",
            )
            assert len(documents) == 1
            assert documents[0]["name"] == "John"

        test_flow._run()


def test_firestore_operation_failure(gcp_credentials_mock):
    with patch(
        "prefect_gcp.firestore.firestore_create_document",
        side_effect=GoogleAPICallError("Error"),
    ):

        @Flow
        def test_flow():
            with pytest.raises(GoogleAPICallError):
                firestore_create_document(
                    collection="users",
                    document_data={"name": "John", "age": 30},
                    gcp_credentials=gcp_credentials_mock,
                    project="my-project",
                    location="us-central1",
                )

        test_flow._run()
