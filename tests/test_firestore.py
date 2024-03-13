import os
import pytest
from prefect import Flow
from unittest.mock import MagicMock
from prefect_gcp.firestore import (
    firestore_create_collection,
    firestore_create_document,
    firestore_delete_document,
    firestore_read_collection,
    firestore_update_document,
)

@pytest.fixture
def gcp_credentials_mock():
    # Create a MagicMock object to mock the behavior of GcpCredentials
    mock_credentials = MagicMock()

    # Mock the behavior of get_firestore_client method to return a MagicMock
    mock_firestore_client = MagicMock()
    mock_credentials.get_firestore_client.return_value = mock_firestore_client

    return mock_credentials

def test_firestore_create_collection(gcp_credentials_mock):
    @Flow
    def test_flow():
        collection_ref = firestore_create_collection(
            collection="users",
            gcp_credentials=gcp_credentials_mock,
            project="my-project",
            location="us-central1",
        )
        return collection_ref

    test_flow._run()

def test_firestore_create_document(gcp_credentials_mock):
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
        return document_ref

    test_flow._run()

def test_firestore_read_collection(gcp_credentials_mock):
    @Flow
    def test_flow():
        documents = firestore_read_collection(
            collection="users",
            gcp_credentials=gcp_credentials_mock,
            project="my-project",
            location="us-central1",
        )
        return documents

    test_flow._run()

def test_firestore_update_document(gcp_credentials_mock):
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
        return updated_doc

    test_flow._run()

def test_firestore_delete_document(gcp_credentials_mock):
    @Flow
    def test_flow():
        firestore_delete_document(
            collection="users",
            document_id="document_id",
            gcp_credentials=gcp_credentials_mock,
            project="my-project",
            location="us-central1",
        )

    test_flow._run()
