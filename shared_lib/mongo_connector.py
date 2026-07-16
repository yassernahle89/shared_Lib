from pymongo import MongoClient
import logging
from bson import ObjectId
from bson.errors import InvalidId

logger = logging.getLogger(__name__)

class MongoWriter:
    def __init__(
        self,
        uri="mongodb://localhost:27017",
        db="testdb",
        collection="messages",
        tls_cert_key_file=None,      # combined client cert + private key (PEM)
        tls_ca_file=None,            # CA cert to verify the server
        tls_cert_key_password=None,  # only if private key is password-protected
    ):
        self.uri = uri
        self.db_name = db
        self.collection_name = collection
        self.tls_cert_key_file = tls_cert_key_file
        self.tls_ca_file = tls_ca_file
        self.tls_cert_key_password = tls_cert_key_password
        self.client = None
        self.collection = None

    def connect(self):
        try:
            client_kwargs = {}

            if self.tls_cert_key_file:
                client_kwargs["tls"] = True
                client_kwargs["tlsCertificateKeyFile"] = self.tls_cert_key_file
                if self.tls_ca_file:
                    client_kwargs["tlsCAFile"] = self.tls_ca_file
                if self.tls_cert_key_password:
                    client_kwargs["tlsCertificateKeyFilePassword"] = self.tls_cert_key_password

            self.client = MongoClient(self.uri, **client_kwargs)
            self.collection = self.client[self.db_name][self.collection_name]

            # Force a round trip so connect() actually fails fast if certs/auth are wrong,
            # instead of failing silently later on first insert
            self.client.admin.command("ping")

            logger.info(
                f"Connected to MongoDB, DB: {self.db_name}, "
                f"Collection: {self.collection_name}, TLS: {bool(self.tls_cert_key_file)}"
            )
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    def insert(self, document: dict):
        if self.collection is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if not document or not isinstance(document, dict):
            logger.warning(f"Skipping invalid document: {document}")
            return

        try:
            if "_id" in document and (document["_id"] is None or document["_id"] == ""):
                document.pop("_id")

            self.collection.insert_one(document)

        except Exception as e:
            logger.error(f"Failed to insert document: {e}")

    def close(self):
        if self.client:
            self.client.close()

    def get(self, doc_id, collection_name=None):
        """
        Fetch a document by its _id, optionally from a different collection
        than the one bound at connect() time.

        collection_name: if provided, queries self.db[collection_name] instead
                          of the default self.collection.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if doc_id is None:
            logger.warning("Skipping get(): doc_id is None")
            return None

        # Resolve which collection to query
        if collection_name:
            collection = self.client[self.db_name][collection_name]
        else:
            if self.collection is None:
                raise RuntimeError("MongoWriter not connected. Call connect() first.")
            collection = self.collection

        query_id = doc_id
        if isinstance(doc_id, str):
            try:
                query_id = ObjectId(doc_id)
            except InvalidId:
                query_id = doc_id

        try:
            return collection.find_one({"_id": query_id})
        except Exception as e:
            logger.error(
                f"Failed to fetch document with _id={doc_id} "
                f"from collection={collection_name or self.collection_name}: {e}"
            )
            return None
# from pymongo import MongoClient
# import logging

# logger = logging.getLogger(__name__)

# class MongoWriter:
#     def __init__(self, uri="mongodb://localhost:27017", db="testdb", collection="messages"):
#         self.uri = uri
#         self.db_name = db
#         self.collection_name = collection
#         self.client = None
#         self.collection = None

#     def connect(self):
#         try:
#             self.client = MongoClient(self.uri)
#             self.collection = self.client[self.db_name][self.collection_name]
#             logger.info(
#                 f"Connected to MongoDB at {self.uri}, "
#                 f"DB: {self.db_name}, Collection: {self.collection_name}"
#             )
#         except Exception as e:
#             logger.error(f"MongoDB connection failed: {e}")
#             raise

#     def insert(self, document: dict):
#         if self.collection is None:
#             raise RuntimeError("MongoWriter not connected. Call connect() first.")

#         if not document or not isinstance(document, dict):
#             logger.warning(f"Skipping invalid document: {document}")
#             return  # don’t crash, just skip

#         try:
#             # Drop empty/invalid _id so Mongo will generate one
#             if "_id" in document and (document["_id"] is None or document["_id"] == ""):
#                 document.pop("_id")

#             self.collection.insert_one(document)

#         except Exception as e:
#             logger.error(f"Failed to insert document: {e}")

#     def close(self):
#         if self.client:
#             self.client.close()
