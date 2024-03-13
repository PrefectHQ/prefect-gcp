import os
from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentReference
from prefect import get_run_logger, task
from typing import Optional, List, Dict, Any
from prefect_gcp.credentials import GcpCredentials


@task
async def firestore_create_collection(
    collection: str,
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: str = "us-central1",
) -> DocumentReference:
    """
    Creates a collection in Firestore.
    Args:
        collection: Name of the collection to create.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Project ID where Firestore exists.
        location: The location of the Firestore instance.
    Returns:
        Reference to the created collection.
    Example:
        Creates a collection named "users" in Firestore using a Prefect flow.
        ```python
        from prefect import Flow
        from prefect_gcp import GcpCredentials
        from prefecr_gcp.firestore import firestore_create_collection

        @Flow()
        def example_firestore_create_collection_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json"
            )
            collection_ref = firestore_create_collection(
                collection="users",
                gcp_credentials=gcp_credentials,
                project="my-project"
            )

        example_firestore_create_collection_flow()
        ```   
    """
    logger = get_run_logger()
    logger.info("Creating collection: %s", collection)

    if not project:
        raise ValueError("Project ID must be provided.")

    firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

    collection_ref = firestore_client.collection(collection)

    # Attempt to create the collection (collections in Firestore are created implicitly)
    return collection_ref


@task
async def firestore_create_document(
    collection: str,
    document_data: Dict[str, Any],
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: str = "us-central1",
) -> DocumentReference:
    """
    Creates a document in a Firestore collection.
    Args:
        collection: Name of the collection to insert the document into.
        document_data: Data to be inserted into the document.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Project ID where Firestore exists.
        location: The location of the Firestore instance.
    Returns:
        Reference to the created document.
    Example:
        Creates a document with data {"name": "John", "age": 30} in the "users" collection using a Prefect flow.
        ```python
        from prefect import Flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.firestore import firestore_create_document

        @Flow()
        def example_firestore_create_document_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json"
            )
            document_data = {"name": "John", "age": 30}
            doc_ref = firestore_create_document(
                collection="users",
                document_data=document_data,
                gcp_credentials=gcp_credentials,
                project="my-project"
            )

        example_firestore_create_document_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Creating document in collection: %s", collection)

    if not project:
        raise ValueError("Project ID must be provided.")

    firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

    collection_ref = firestore_client.collection(collection)
    doc_ref = await collection_ref.add(document_data)

    logger.info("Document created: %s", doc_ref.id)
    return doc_ref


@task
async def firestore_read_collection(
    collection: str,
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: str = "us-central1",
) -> List[Dict[str, Any]]:
    """
    Reads all documents from a Firestore collection.
    Args:
        collection: Name of the collection to read documents from.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Project ID where Firestore exists.
        location: The location of the Firestore instance.
    Returns:
        List of dictionaries representing the documents.
    Example:
        Reads all documents from the "users" collection in Firestore using a Prefect flow.
        ```python
        from prefect import Flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.firestore import firestore_read_collection

        @Flow()
        def example_firestore_read_collection_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json"
            )
            documents = firestore_read_collection(
                collection="users",
                gcp_credentials=gcp_credentials,
                project="my-project"
            )
            for document in documents:
                print(document)

        example_firestore_read_collection_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Reading documents from collection: %s", collection)

    if not project:
        raise ValueError("Project ID must be provided.")

    firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

    collection_ref = firestore_client.collection(collection)
    documents = await collection_ref.get()

    documents_data = []
    for doc in documents:
        documents_data.append(doc.to_dict())

    logger.info("Read %s documents from collection: %s", len(documents_data), collection)
    return documents_data


@task
async def firestore_update_document(
    collection: str,
    document_id: str,
    update_data: Dict[str, Any],
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: str = "us-central1",
) -> Dict[str, Any]:
    """
    Updates a document in a Firestore collection.
    Args:
        collection: Name of the collection containing the document.
        document_id: ID of the document to be updated.
        update_data: Data to be updated in the document.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Project ID where Firestore exists.
        location: The location of the Firestore instance.
    Returns:
        Dictionary representing the updated document.
    Example:
        Updates a document with ID "123" in the "users" collection with new data {"age": 35} using a Prefect flow.
        ```python
        from prefect import Flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.firestore import firestore_update_document

        @Flow()
        def example_firestore_update_document_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json"
            )
            update_data = {"age": 35}
            updated_doc = firestore_update_document(
                collection="users",
                document_id="123",
                update_data=update_data,
                gcp_credentials=gcp_credentials,
                project="my-project"
            )
            print(updated_doc)

        example_firestore_update_document_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Updating document %s in collection: %s", document_id, collection)

    if not project:
        raise ValueError("Project ID must be provided.")

    firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

    collection_ref = firestore_client.collection(collection)
    doc_ref = collection_ref.document(document_id)

    await doc_ref.update(update_data)

    updated_doc = await doc_ref.get()
    updated_doc_data = updated_doc.to_dict()

    logger.info("Document updated: %s", updated_doc_data)
    return updated_doc_data


@task
async def firestore_delete_document(
    collection: str,
    document_id: str,
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: str = "us-central1",
) -> None:
    """
    Deletes a document from a Firestore collection.
    Args:
        collection: Name of the collection containing the document.
        document_id: ID of the document to be deleted.
        gcp_credentials: Credentials to use for authentication with GCP.
        project: Project ID where Firestore exists.
        location: The location of the Firestore instance.
    Returns:
        None
    Example:
        Deletes a document with ID "123" from the "users" collection using a Prefect flow.
        ```python
        from prefect import Flow
        from prefect_gcp import GcpCredentials
        from prefect_gcp.firestore import firestore_delete_document

        @Flow()
        def example_firestore_delete_document_flow():
            gcp_credentials = GcpCredentials(
                service_account_file="/path/to/service/account/keyfile.json"
            )
            firestore_delete_document(
                collection="users",
                document_id="123",
                gcp_credentials=gcp_credentials,
                project="my-project"
            )

        example_firestore_delete_document_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Deleting document %s from collection: %s", document_id, collection)

    if not project:
        raise ValueError("Project ID must be provided.")

    firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

    collection_ref = firestore_client.collection(collection)
    doc_ref = collection_ref.document(document_id)

    await doc_ref.delete()

    logger.info("Document %s deleted from collection: %s", document_id, collection)


