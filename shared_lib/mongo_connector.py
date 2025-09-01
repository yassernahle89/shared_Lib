from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

class MongoWriter:
    def __init__(self, uri="mongodb://localhost:27017", db="testdb", collection="messages"):
        self.uri = uri
        self.db_name = db
        self.collection_name = collection
        self.client = None
        self.collection = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.collection = self.client[self.db_name][self.collection_name]
            logger.info(
                f"Connected to MongoDB at {self.uri}, "
                f"DB: {self.db_name}, Collection: {self.collection_name}"
            )
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    def insert(self, document: dict):
        if self.collection in None:
            raise RuntimeError("MongoWriter not connected. Call connect() first.")
        try:
            # self.collection.insert_one(document)
                    # Drop empty/invalid _id so Mongo will generate one
            if "_id" in document and (document["_id"] is None or document["_id"] == ""):
                document.pop("_id")
            self.collection.insert_one(document)
        except Exception as e:
            logger.error(f"Failed to insert document: {e}")

    def close(self):
        if self.client:
            self.client.close()
