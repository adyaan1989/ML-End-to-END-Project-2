import sys
import os
import logging
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from typing import Tuple
from shipment.exception import shippingException
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.config_entity import DataIngestionConfig
from shipment.entity.artifacts_entity import DataIngestionArtifacts
from shipment.constant import TEST_SIZE


class DataIngestion:
    def __init__(
            
        self, data_ingestion_config: DataIngestionConfig, mongo_op: MongoDBOperation
    ):
        self.data_ingestion_config = data_ingestion_config
        self.mongo_op = mongo_op

    def get_data_from_mongodb(self) -> DataFrame:
        logging.info("Entered get_data_from_mongodb method of DataIngestion class")
        try:
            logging.info("Getting data from the dataframe mongodb")

            df = self.mongo_op.get_collection_as_dataframe(
                self.data_ingestion_config.DB_NAME,
                self.data_ingestion_config.COLLECTION_NAME,
            )
            logging.info("received the data from mongodb")
            logging.info("Exited to get_data_from_mongodb method of DataIngestion")

            return df
        
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def split_data_as_train_test(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        logging.info("Entered split_data_as_train_test method of DataIngestion class")
        try:
            os.makedirs(
                self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR, exist_ok=True)
            
            train_set, test_set = train_test_split(df, test_size=TEST_SIZE)
            logging.info("Performed train and test split on datafram")

            os.makedirs(
                self.data_ingestion_config.TRAIN_DATA_ARTIFACTS_FILE_DIR, exist_ok=True)
            logging.info(f"created {os.path.basename(self.data_ingestion_config.TRAIN_DATA_ARTIFACTS_FILE_DIR)} directory")

        
            os.makedirs(
                self.data_ingestion_config.TEST_DATA_ARTIFACTS_FILE_DIR, exist_ok=True)
            logging.info(f"created {os.path.basename(self.data_ingestion_config.TEST_DATA_ARTIFACTS_FILE_DIR)} directory")

            # Debug logs to verify paths
            logging.info(f"Train data file path: {self.data_ingestion_config.TRAIN_DATA_FILE_PATH}")
            logging.info(f"Test data file path: {self.data_ingestion_config.TEST_DATA_FILE_PATH}")

            train_set.to_csv(self.data_ingestion_config.TRAIN_DATA_FILE_PATH, index = False, header =True)
            test_set.to_csv(self.data_ingestion_config.TEST_DATA_FILE_PATH, index = False, header =True)

            logging.info("Converted train DataFram and Test DataFrame to CSV")
            logging.info(
                f"saved {os.path.basename(self.data_ingestion_config.TRAIN_DATA_FILE_PATH)}, \
                    {os.path.basename(self.data_ingestion_config.TEST_DATA_FILE_PATH)} in \
                        {os.path.basename(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR)}."
            )

            logging.info("Exited to split_data_as_train_test of Data_Ingestion class")            

            return train_set, test_set

        except Exception as e:
            raise shippingException(e, sys) from e
                
    def initiate_data_ingestion(self) -> DataIngestionArtifacts:
        logging.info("Entered the initiate_data_ingestion method of DataIngestion")
        try:
            df = self.get_data_from_mongodb()

            #Droping the unnecessary fields
            df1 = df.drop(self.data_ingestion_config.DROP_COLUMNS, axis=1)
            df1 = df1.dropna()
            logging.info("got the data from mongodb")

            self.split_data_as_train_test(df1)

            Data_Ingestion_Artifacts = DataIngestionArtifacts(
                train_data_file_path=self.data_ingestion_config.TRAIN_DATA_FILE_PATH,
                test_data_file_path=self.data_ingestion_config.TEST_DATA_FILE_PATH,
            )
            return Data_Ingestion_Artifacts
        
        except Exception as e:
            raise shippingException(e, sys) from e
        

