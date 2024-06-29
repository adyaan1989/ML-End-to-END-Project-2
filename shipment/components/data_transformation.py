import sys
from shipment.logger import logging
from pandas import DataFrame
import numpy as np
import pandas as pd
from category_encoders.binary import BinaryEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from shipment.entity.config_entity import DataTransformationConfig
from shipment.entity.artifacts_entity import (
    DataIngestionArtifacts,
    DataTransformationArtifacts
)

from shipment.exception import shippingException

from shipment.utils.main_utils import MainUtils
from shipment.constant import *


class DataTransformation:
    def __init__(
            self,
            data_ingestion_artifacts = DataIngestionArtifacts,
            data_transformation_config = DataTransformationConfig,
    ):  
        self.data_ingestion_artifacts = data_ingestion_artifacts
        self.data_transformation_config = data_transformation_config

        self.train_set = pd.read_csv(self.data_ingestion_artifacts.train_data_file_path)
        self.test_set = pd.read_csv(self.data_ingestion_artifacts.test_data_file_path)
    
    def get_data_transformation_objects(self) -> object:
        logging.info("Entered get_data_transformation_objects of DataIngection Class")
        try:
            numerical_columns = self.data_transformation_config.SCHEMA_CONFIG["numerical_columns"]
            onehot_columns = self.data_transformation_config.SCHEMA_CONFIG["onehot_columns"]
            binary_columns = self.data_transformation_config.SCHEMA_CONFIG["binary_columns"]
            logging.info("got the numerical columns, onehot columns and binary columns from schema.yaml file")

            # lets create the transformation objects
            numerical_transformer = StandardScaler()
            oh_transformer = OneHotEncoder(handle_unknown="ignore")
            binary_transformer = BinaryEncoder()
            logging.info("Initialized StanderScaler, OneHotEncoder and BinaryEncoder")

            # Using transformer objects in column transformer
            preprocessor = ColumnTransformer(
                [
                    ("OneHotEncoder", oh_transformer, onehot_columns),
                    ("BinaryEncoder", binary_transformer, binary_columns),
                    ("StandardScaler", numerical_transformer, numerical_columns),
                ]
            )
            logging.info("created the preprocessor object from column transformer")
            logging.info("Exited get_data_transformation_objects of DataTransformation class")
            return preprocessor

        except Exception as e:
            raise shippingException(e, sys) from e

    def _outlier__capping(self, col, df: DataFrame) -> DataFrame:
        logging.info("Enterted the outerlier capping method of DtatTransformation class")
        try:
            logging.info("Performing the outerlier capping for columns in dataframe")
            percentile25 = df[col].quantile(0.25)
            percentile75 = df[col].quantile(0.75)

            # fix the upper * lower bound
            iqr = percentile75 - percentile25
            upperlimit = percentile75 + 1.5 * iqr
            lowerlimit = percentile25 - 1.5 * iqr

            # capping the outliers
            df.loc[(df[col] > upperlimit), col] = upperlimit
            df.loc[(df[col] < lowerlimit), col] = lowerlimit
            logging.info("Completed the outlier capping method of DataTransformation class")
            return df
        
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def initiate_data_transformation(self) -> DataTransformationArtifacts:
        logging.info("Enteted the initial data transformation method of DataTransformation class")

        try:
            os.makedirs(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR, exist_ok=True)
            logging.info(f"created artifacts directory: for {os.path.basename(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR)}")

            preprocessor = self.get_data_transformation_objects()
            logging.info(f"got the peeprocessor object")

            target_column_name = self.data_transformation_config.SCHEMA_CONFIG["target_column"]

            numerical_columns = self.data_transformation_config.SCHEMA_CONFIG["numerical_columns"]

            logging.info(f"got the target column name and numerical columns name: from schema config")

            # outlier capping
            continuoues_columns = [
                feature
                for feature in numerical_columns
                if len(self.train_set[feature].unique()) >= 25
            ]
            logging.info(f"got a list of continuous column names")
                
            [self._outlier__capping(col, self.train_set) for col in continuoues_columns]
            logging.info(f"outlier capped in train df")
            [self._outlier__capping(col, self.test_set) for col in continuoues_columns]
            
            logging.info(f"outlier capped in test df")
            
            # Getting the input features and target features of training dataset
            input_feature_train_df = self.train_set.drop(columns=[target_column_name], axis=1)
            target_feature_train_df = self.train_set[target_column_name]
            logging.info(f"got the train_x and train_y features from dataset")

            input_feature_test_df = self.test_set.drop(columns=[target_column_name], axis=1)
            target_feature_test_df = self.test_set[target_column_name]
            logging.info(f"got the test_x and test_y features of from dataset")
            
            # let apply the preprocessing object on training & testing datasets
            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessor.transform(input_feature_test_df)
            logging.info(f"used the preprocessing object on training & test datasets to transform the features")

            #Concatinating the input feature array and target feature array of training dataset
            train_arr = np.c_[
                input_feature_train_arr, np.array(target_feature_train_df)
                ]
            logging.info(f"created the train array")
            
            # creating the directory for transformed train dataset array and saving the transformed data
            os.makedirs(self.data_transformation_config.TRANSFORMED_TRAIN_DATA_DIR, exist_ok=True)

            transformed_train_file = self.data_transformation_config.UTILS.save_numpy_array_data(
                self.data_transformation_config.TRANSFORMED_TRAIN_FILE_PATH, train_arr
            )
            logging.info(f"saved train array to {os.path.basename(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR)}")

            #Concatinating the input feature array and target feature array of test dataset
            test_arr = np.c_[
                input_feature_test_arr, np.array(target_feature_test_df)
                ]
            logging.info(f"created the test array")
            
            # creating the directory for transformed train dataset array and saving the transformed data
            os.makedirs(self.data_transformation_config.TRANSFORMED_TEST_DATA_DIR, exist_ok=True)

            transformed_test_file = self.data_transformation_config.UTILS.save_numpy_array_data(
                self.data_transformation_config.TRANSFORMED_TEST_FILE_PATH, test_arr
            )
            logging.info(f"saved train array to {os.path.basename(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR)}")

            preprocessor_obj_file = self.data_transformation_config.UTILS.save_object(
                self.data_transformation_config.PREPROCESSOR_FILE_PATH, preprocessor
            )
            logging.info(f"saved the preprocessor object in DataTransformationArtifact directory")
            logging.info(f"Exited to initate_date_transformation method of data_transformation class")

            # Saving data transformation artifacts
            data_transformation_artifacts = DataTransformationArtifacts(
                transformed_object_file_path = preprocessor_obj_file,
                transformed_train_file_path = transformed_train_file,
                transformed_test_file_path = transformed_test_file
            )
            return data_transformation_artifacts
        
        except Exception as e:
            raise shippingException(e, sys) from e
        
