from pymongo import MongoClient
from pymongo.operations import SearchIndexModel
import logging
import uuid
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

    def insert_many(self, documents: list, collection_name: str) -> bool:
        """
        Insert a list of documents into the given collection in a single
        multi-document transaction (all-or-nothing), not one insert at a
        time. Each document is expected to already carry its own `_id`.

        Requires MongoDB to be a replica set or sharded cluster — standalone
        deployments don't support transactions.

        Returns True if the transaction committed successfully.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if not documents or not isinstance(documents, list):
            logger.warning(f"Skipping insert_many(): invalid documents {documents}")
            return False

        collection = self.client[self.db_name][collection_name]

        try:
            with self.client.start_session() as session:
                with session.start_transaction():
                    collection.insert_many(documents, session=session)

            logger.info(f"Inserted {len(documents)} documents into {collection_name} (transaction committed)")
            return True
        except Exception as e:
            logger.error(f"Failed to insert {len(documents)} documents into {collection_name}: {e}")
            raise

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

    def update(self, doc_id, fields: dict, collection_name=None) -> bool:
        """
        Update specific fields on a document by its _id (uses $set).
        Returns True if a document was actually matched and modified.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if doc_id is None:
            logger.warning("Skipping update(): doc_id is None")
            return False

        if not fields or not isinstance(fields, dict):
            logger.warning(f"Skipping update(): invalid fields {fields}")
            return False

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
            result = collection.update_one({"_id": query_id}, {"$set": fields})
            return result.modified_count > 0
        except Exception as e:
            logger.error(
                f"Failed to update document with _id={doc_id} "
                f"from collection={collection_name or self.collection_name}: {e}"
            )
            raise
    
    def update_topics_and_clusters(self, doc_id, cluster_keywords: list, collection_name="batch") -> bool:
        """
        Update the `Topics` and `Clusters` fields on a batch document by its
        _id, and set `status` to "AwaitingHumanReview". Builds the
        Topic/Cluster objects from the given data and replaces each list
        wholesale.

        cluster_keywords: list of dicts, one per cluster, e.g.:
            [
                {"cluster": 20, "keywords": ["privacy", "policy", ...], "count": 123},
                {"cluster": 12, "keywords": ["website", "using", ...], "count": 45},
            ]
            Each entry produces one Cluster (carrying that cluster's
            `count` as `Count`) and one Topic under that cluster, with the
            entry's full keyword list stored on the Topic's Keywords field.

        Returns True if a document was actually matched and modified.
        """
        topic_docs = []
        cluster_docs = []

        for entry in cluster_keywords:
            cluster_num = entry["cluster"]
            keywords = entry["keywords"]
            count = entry["count"]

            cluster_id = str(uuid.uuid4())
            cluster_docs.append({
                "ClusterId": cluster_id,
                "Name": str(cluster_num),
                "Count": count,
                "IsActive": True,
            })

            topic_docs.append({
                "TopicId": str(uuid.uuid4()),
                "Label": f"Topic_{cluster_num}",
                "ClusterId": cluster_id,
                "Keywords": keywords,
                "Score": 0.0,
                "IsActive": True,
            })

        return self.update(
            doc_id,
            {"Topics": topic_docs, "Clusters": cluster_docs, "status": "AwaitingHumanReview"},
            collection_name=collection_name,
        )

    def get_active_clusters(self, doc_id, collection_name="batch") -> list:
        """
        Fetch a batch document by its _id and return the names of its
        active clusters (Cluster.IsActive == True).

        Returns a list of cluster name strings (empty if the document
        doesn't exist or has no active clusters).
        """
        document = self.get(doc_id, collection_name=collection_name)
        if not document:
            logger.warning(f"Skipping get_active_clusters(): document not found for _id={doc_id}")
            return []

        return [
            cluster["Name"]
            for cluster in document.get("Clusters", [])
            if cluster.get("IsActive")
        ]

    def find(self, query: dict, collection_name=None) -> list:
        """
        Fetch multiple documents matching a query, optionally from a
        different collection than the one bound at connect() time.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if collection_name:
            collection = self.client[self.db_name][collection_name]
        else:
            if self.collection is None:
                raise RuntimeError("MongoWriter not connected. Call connect() first.")
            collection = self.collection

        try:
            return list(collection.find(query))
        except Exception as e:
            logger.error(
                f"Failed to query documents with query={query} "
                f"from collection={collection_name or self.collection_name}: {e}"
            )
            return []


    def CreateSearchIndex(self, searchIndexName: str, collection_name=None) -> bool:
        """
        Create a MongoDB Atlas vector search index on the given collection.
        `numDimensions` is inferred from the length of the `embedding`
        field on the collection's first document.

        Returns True if the index was created successfully.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if collection_name:
            collection = self.client[self.db_name][collection_name]
        else:
            if self.collection is None:
                raise RuntimeError("MongoWriter not connected. Call connect() first.")
            collection = self.collection

        sample_doc = collection.find_one({}, {"embedding": 1})
        if not sample_doc or "embedding" not in sample_doc:
            raise ValueError(
                f"Cannot determine vector_dim: no document with an 'embedding' field "
                f"found in collection={collection_name or self.collection_name}"
            )

        vector_dim = len(sample_doc["embedding"])

        search_index_model = SearchIndexModel(
            definition={
                "fields": [
                    {
                        "type": "vector",
                        "numDimensions": vector_dim,
                        "path": "embedding",
                        "similarity": "cosine",
                    },
                    {
                        "type": "filter",
                        "path": "metadata.name",
                    },
                    {
                        "type": "filter",
                        "path": "metadata.roles",
                    },
                ]
            },
            name=searchIndexName,
            type="vectorSearch",
        )

        try:
            collection.create_search_index(model=search_index_model)
            logger.info(
                f"Created vector search index '{searchIndexName}' on "
                f"collection={collection_name or self.collection_name} (vector_dim={vector_dim})"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to create search index '{searchIndexName}': {e}")
            raise

    def SearchIndexExists(self, searchIndexName: str, collection_name=None) -> bool:
        """
        Check whether a search index with the given name already exists on
        the collection. Call this before CreateSearchIndex() to avoid a
        duplicate-index error.
        """
        if self.client is None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")

        if collection_name:
            collection = self.client[self.db_name][collection_name]
        else:
            if self.collection is None:
                raise RuntimeError("MongoWriter not connected. Call connect() first.")
            collection = self.collection

        try:
            return any(collection.list_search_indexes(searchIndexName))
        except Exception as e:
            logger.error(f"Failed to check search index '{searchIndexName}': {e}")
            raise

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
