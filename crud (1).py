# crud.py

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, PyMongoError
from typing import Any, Dict, List, Optional


class CRUD:
    """
    CRUD operations for MongoDB collections.
    """

    def __init__(self, user: str, password: str, host: str, port: int, db_name: str, collection_name: str):
        """
        Initialize the MongoDB client and specify the database and collection.

        Connection Variables:
            user (str): "aacuser"
            password (str): "jackelope"
            host (str): "nv-desktop-services.apporto.com"
            port (int): 31580
            db_name (str): "aac"
            collection_name (str): "animals"
        """
        try:
            # Construct the MongoDB URI with authentication
            uri = f'mongodb://{user}:{password}@{host}:{port}/'
            self.client = MongoClient(uri)
            
            # Access the specified database
            self.database = self.client[db_name]
            
            # Access the specified collection
            self.collection = self.database[collection_name]
            
            # Test the connection
            self.client.admin.command('ping')
            print("MongoDB connection established successfully.")
        except ConnectionFailure as cf:
            print(f"Could not connect to MongoDB: {cf}")
            raise
        except OperationFailure as of:
            print(f"Authentication failed: {of}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    def create(self, document: Dict[str, Any]) -> bool:
        """
        Insert a document into the MongoDB collection.

        Parameters:
            document (dict): The document to be inserted.

        Returns:
            bool: True if insertion is successful, False otherwise.
        """
        try:
            if not isinstance(document, dict):
                print("Invalid document format. Document must be a dictionary.")
                return False

            result = self.collection.insert_one(document)
            if result.acknowledged:
                print(f"Document inserted with _id: {result.inserted_id}")
                return True
            else:
                print("Insertion not acknowledged by MongoDB.")
                return False
        except PyMongoError as e:
            print(f"An error occurred during document insertion: {e}")
            return False

    def read(self, query: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Query documents from the MongoDB collection.

        Parameters:
            query (dict, optional): The query filter. If None, retrieves all documents.

        Returns:
            list: A list of matching documents or an empty list if none found.
        """
        try:
            if query is None:
                print("No query provided. Retrieving all documents.")
                cursor = self.collection.find()
            else:
                if not isinstance(query, dict):
                    print("Invalid query format. Query must be a dictionary.")
                    return []
                print(f"Retrieving documents with query: {query}")
                cursor = self.collection.find(query)
            
            results = list(cursor)
            print(f"Number of documents retrieved: {len(results)}")
            return results
        except PyMongoError as e:
            print(f"An error occurred during document retrieval: {e}")
            return []

    def update(self, query: Dict[str, Any], new_values: Dict[str, Any], multiple: bool = False) -> int:
        """
        Update documents in the MongoDB collection based on a query.

        Parameters:
            query (dict): The query filter to select documents.
            new_values (dict): The key/value pairs to update.
            multiple (bool): If True, updates all matching documents. If False, updates only one.

        Returns:
            int: The number of documents modified.
        """
        try:
            if not isinstance(query, dict) or not isinstance(new_values, dict):
                print("Invalid input. Both query and new_values must be dictionaries.")
                return 0

            if multiple:
                result = self.collection.update_many(query, new_values)
                modified_count = result.modified_count
                print(f"Number of documents updated: {modified_count}")
                return modified_count
            else:
                result = self.collection.update_one(query, new_values)
                modified_count = result.modified_count
                print(f"Number of documents updated: {modified_count}")
                return modified_count
        except PyMongoError as e:
            print(f"An error occurred during document update: {e}")
            return 0

    def delete(self, query: Dict[str, Any], multiple: bool = False) -> int:
        """
        Delete documents from the MongoDB collection based on a query.

        Parameters:
            query (dict): The query filter to select documents.
            multiple (bool): If True, deletes all matching documents. If False, deletes only one.

        Returns:
            int: The number of documents deleted.
        """
        try:
            if not isinstance(query, dict):
                print("Invalid query format. Query must be a dictionary.")
                return 0

            if multiple:
                result = self.collection.delete_many(query)
                deleted_count = result.deleted_count
                print(f"Number of documents deleted: {deleted_count}")
                return deleted_count
            else:
                result = self.collection.delete_one(query)
                deleted_count = result.deleted_count
                print(f"Number of documents deleted: {deleted_count}")
                return deleted_count
        except PyMongoError as e:
            print(f"An error occurred during document deletion: {e}")
            return 0

    def __del__(self):
        """
        Destructor to ensure the MongoDB client is properly closed when the object is deleted.
        """
        try:
            self.client.close()
            print("MongoDB connection closed.")
        except AttributeError:
            # If the client was never created due to an exception in __init__
            pass

