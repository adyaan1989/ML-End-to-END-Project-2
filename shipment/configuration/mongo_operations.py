import sys
from json import loads
from typing import Collection
import pandas as pd
from pandas import DataFrame
from pymongo.database import Database
from pymongo import MongoClient
from shipment.constant import DB_URL
from shipment.exception import shippingException
from shipment.logger import logging

class MongoDBOperation:
    def __init__(self):
        self.DB_URL = DB_URL
        self.client = MongoClient(self.DB_URL)

    def get_database(self, db_name) -> Database:
        logging.info("Entered get_database method of MongoDBOperation class.")
        try:
            db = self.client[db_name]
            logging.info(f"Created {db_name} database in MongoDB.")
            logging.info("Exited get_database method of MongoDBOperation class.")
            return db
        except Exception as e:
            raise shippingException(e, sys) from e

    @staticmethod
    def get_collection(database, collection_name) -> Collection:
        logging.info("Entered get_collection method of MongoDBOperation class.")
        try:
            collection = database[collection_name]
            logging.info(f"Created {collection_name} collection in MongoDB.")
            logging.info("Exited get_collection method of MongoDBOperation class.")
            return collection
        except Exception as e:
            raise shippingException(e, sys) from e

    def get_collection_as_dataframe(self, db_name, collection_name) -> DataFrame:
        logging.info("Entered get_collection_as_dataframe method of MongoDBOperation class.")
        try:
            database = self.get_database(db_name)
            collection = database.get_collection(collection_name)

            # Fetch all documents from the collection and convert them to a DataFrame
            df = pd.DataFrame(list(collection.find()))
            # Drop the '_id' column if it exists
            if "_id" in df.columns:
                df = df.drop(columns=["_id"], axis=1)

            logging.info("Converted collection to DataFrame")
            logging.info("Exited get_collection_as_dataframe method of MongoDBOperation class.")
            return df
        except Exception as e:
            raise shippingException(e, sys) from e

    def insert_dataframe_as_record(self, data_frame, db_name, collection_name) -> None:
        logging.info("Entered insert_dataframe_as_record method of MongoDBOperation class.")
        try:
            records = loads(data_frame.T.to_json()).values()
            logging.info(f"Converted DataFrame to JSON records")

            database = self.get_database(db_name)
            collection = database.get_collection(collection_name)
            collection.insert_many(records)  # Insert the records into the collection

            logging.info("Inserted records to MongoDB")
            logging.info("Exited insert_dataframe_as_record method of MongoDBOperation class.")
        except Exception as e:
            raise shippingException(e, sys) from e
