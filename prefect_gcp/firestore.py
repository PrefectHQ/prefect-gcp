import os
from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentReference
from prefect import get_run_logger, task
from typing import Optional, List, Dict, Any
from typing import Union
from prefect_gcp.credentials import GcpCredentials


@task
async def firestore_create_collection(
    collection: str,
    gcp_credentials: GcpCredentials,
    project: Optional[str] = None,
    location: Optional[str] = None,
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
    location: Optional[str] = None,
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
    location: Optional[str] = None,
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
    location: Optional[str] = None,
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
    location: Optional[str] = None,
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

    #add read document task
@task
async def firestore_read_document(
        collection: str,
        document_id: str,
        gcp_credentials: GcpCredentials,
        project: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Reads a document from a Firestore collection.
        Args:
            collection: Name of the collection containing the document.
            document_id: ID of the document to read.
            gcp_credentials: Credentials to use for authentication with GCP.
            project: Project ID where Firestore exists.
            location: The location of the Firestore instance.
        Returns:
            Dictionary representing the document.
        Example:
            Reads a document with ID "123" from the "users" collection using a Prefect flow.
            ```python
            from prefect import Flow
            from prefect_gcp import GcpCredentials
            from prefect_gcp.firestore import firestore_read_document

            @Flow()
            def example_firestore_read_document_flow():
                gcp_credentials = GcpCredentials(
                    service_account_file="/path/to/service/account/keyfile.json"
                )
                document_data = firestore_read_document(
                    collection="users",
                    document_id="123",
                    gcp_credentials=gcp_credentials,
                    project="my-project"
                )
                print(document_data)

            example_firestore_read_document_flow()
            ```
        """
        if not project:
            raise ValueError("Project ID must be provided.")

        firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

        collection_ref = firestore_client.collection(collection)
        doc_ref = collection_ref.document(document_id)

        document = await doc_ref.get()
        document_data = document.to_dict()

        return document_data

    #add query collection task - default false = paramter / option if false return the id from query or if true the whole ducument & a limit on how many can be returned 
@task
async def firestore_query_collection(
        collection: str,
        gcp_credentials: GcpCredentials,
        query_key: str,
        query_value: Any,
        return_whole_document: bool = False,
        project: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Queries a Firestore collection for a specific value.
        Args:
            collection: Name of the collection to query.
            gcp_credentials: Credentials to use for authentication with GCP.
            query_key: Key to query on.
            query_value: Value to query for.
            return_whole_document: If True, returns the entire document. If False, returns only the document ID.
            project: Project ID where Firestore exists.
            location: The location of the Firestore instance.
        Returns:
            List of document IDs or list of dictionaries representing the documents, depending on the value of return_whole_document.
        Example:
            Queries the "users" collection in Firestore for documents where "age" equals 30, returning only the document IDs.
            ```python
            from prefect import Flow
            from prefect_gcp import GcpCredentials
            from prefect_gcp.firestore import firestore_query_collection

            @Flow()
            def example_firestore_query_collection_flow():
                gcp_credentials = GcpCredentials(
                    service_account_file="/path/to/service/account/keyfile.json"
                )
                document_ids = firestore_query_collection(
                    collection="users",
                    query_key="age",
                    query_value=30,
                    return_whole_document=False,
                    gcp_credentials=gcp_credentials,
                    project="my-project"
                )
                for document_id in document_ids:
                    print(document_id)

            example_firestore_query_collection_flow()
            ```
        """
        logger = get_run_logger()
        logger.info("Querying collection: %s", collection)

        if not project:
            raise ValueError("Project ID must be provided.")

        firestore_client = gcp_credentials.get_firestore_client(project=project, location=location)

        query = firestore_client.collection(collection).where(query_key, "==", query_value)
        query.limit(50)

        documents = await query.get()

        if return_whole_document:
            documents_data = [doc.to_dict() for doc in documents]
        else:
            documents_data = [doc.id for doc in documents]

        logger.info("Query returned %s documents", len(documents_data))
        return documents_data






