import os
import sys
import json
import logging
import pandas as pd
from pandas import DataFrame
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection
from typing import Tuple, Union
from shipment.entity.config_entity import DataValidationConfig
from shipment.exception import shippingException
from shipment.entity.artifacts_entity import (
    DataIngestionArtifacts,
    DataValidationArtifacts,
)


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifacts: DataIngestionArtifacts,
        data_validation_config: DataValidationConfig,
    ):

        self.data_ingestion_artifact = data_ingestion_artifacts
        self.data_validation_config = data_validation_config

    def validate_schema_columns(self, df: DataFrame) -> bool:
        try:
            if len(df.columns) == len(
                    self.data_validation_config.SCHEMA_CONFIG['columns']
            ):
                validation_status = True
            else:
                validation_status = False
            return validation_status
        
        except Exception as e:
            raise shippingException(e, sys) from e
    
    def is_numerical_column_exists(self, df: DataFrame) -> bool:
        try:
            validation_status = False
            
            for column in self.data_validation_config.SCHEMA_CONFIG[
                "numerical_columns"
            ]:
                if column not in df.columns:
                    logging.info(f"Numerical column - {column} is not found in DataFrame")
                else:
                    validation_status = True
            return validation_status
        
        except Exception as e:
            raise shippingException(e, sys) from e

    
    def is_categorical_column_exists(self, df: DataFrame) -> bool:
        try:
            validation_status = False
            
            for column in self.data_validation_config.SCHEMA_CONFIG[
                "categorical_columns"
            ]:
                if column not in df.columns:
                    logging.info(f"Categorical column - {column} is not found in DataFrame")
                else:
                    validation_status = True
            return validation_status
        
        except Exception as e:
            raise shippingException(e, sys) from e
            

    def validate_dataset_schema_columns(self) -> Tuple[bool, bool]:
        logging.info("Entered validate_dataset_schema_columns method of DataValidation class from data_validation.py")

        try:
            logging.info("Validating dataset schema columns")

            train_schema_status = self.validate_schema_columns(self.train_set)
            logging.info("validated dataset schema columns on train dataset")

            test_schema_status = self.validate_schema_columns(self.test_set)
            logging.info("validated dataset schema columns on test dataset")

            logging.info("train & test datasets validated")

            return train_schema_status, test_schema_status
        
        except Exception as e:
            raise shippingException(e, sys) from e


    def validate_is_numerical_column_exists(self) -> Tuple[bool, bool]:
        logging.info("Entered validate_is_numerical_column_exists method of DataValidation class from data_validation.py")

        try:
            logging.info("Validating dataset schema for numerical columns")

            train_num_datatype_status = self.is_numerical_column_exists(self.train_set)
            logging.info("validated dataset schema for numerical columns on train dataset")

            test_num_datatype_status = self.is_numerical_column_exists(self.test_set)
            logging.info("validated dataset schema for numerical columns on test dataset")

            logging.info("train & test datasets of numerical features are validated")

            return train_num_datatype_status, test_num_datatype_status
        
        except Exception as e:
            raise shippingException(e, sys) from e
            

    def validate_is_categorical_column_exists(self) -> Tuple[bool, bool]:
        logging.info("Entered validate_is_categorical_column_exists method of DataValidation class from data_validation.py")

        try:
            logging.info("Validating dataset schema for categorical columns")

            train_cat_datatype_status = self.is_categorical_column_exists(self.train_set)
            logging.info("validated dataset schema for categorical columns on train dataset")

            test_cat_datatype_status = self.is_categorical_column_exists(self.test_set)
            logging.info("validated dataset schema for categorical columns on test dataset")

            logging.info("Validated train & test datasets of categorical features are validated")

            return train_cat_datatype_status, test_cat_datatype_status
        
        except Exception as e:
            raise shippingException(e, sys) from e
                    
    
    def detect_dataset_drift(
        self, reference: DataFrame, production: DataFrame, get_ratio: bool = False
    ) -> Union[bool, float]:
            
    
            try:
                data_drift_profile = Profile(sections=[DataDriftProfileSection()])
                data_drift_profile.calculate(reference, production)

                report = data_drift_profile.json()
                json_report = json.loads(report)

                data_drift_file_path = self.data_validation_config.DATA_DRIFT_FILE_PATH
                self.data_validation_config.UTILS.write_json_to_yaml_file(
                    json_report, data_drift_file_path
                )

                n_features = json_report["data_drift"]["data"]["metrics"]["n_features"]
                n_drifted_features = json_report["data_drift"]["data"]["metrics"]["n_drifted_features"]

                if get_ratio:
                    return n_drifted_features / n_features

                else:
                    return json_report["data_drift"]["data"]["metrics"]["dataset_drift"]
                
            except Exception as e:
                raise shippingException(e, sys) from e

    def initiate_data_validation(self) -> DataValidationArtifacts:
        logging.info("Starting initiate_data_validation method of DataValidation class")

        try:
            self.train_set = pd.read_csv(
                self.data_ingestion_artifact.train_data_file_path
            )
            self.test_set = pd.read_csv(
                self.data_ingestion_artifact.test_data_file_path
            )

            logging.info("initated data validation")

            os.makedirs(
                self.data_validation_config.DATA_VALIDATION_ARTIFACTS_DIR, exist_ok=True
            )
            
            logging.info(f"created artifacts directory for {os.path.basename(self.data_validation_config.DATA_VALIDATION_ARTIFACTS_DIR)}")

            drift = self.detect_dataset_drift(self.train_set, self.test_set)
            (
                schema_train_col_status,
                schema_test_col_status,
            ) = self.validate_dataset_schema_columns()

            logging.info(f"schema_train_col_status: {schema_train_col_status} and schema_test_col_status: {schema_test_col_status} ")
            logging.info("validated dataset schema columns")

            (
                schema_train_cat_col_status,
                schema_test_cat_col_status,
            ) = self.validate_is_categorical_column_exists()

            logging.info(f"schema_train_cat_col_status: {schema_train_cat_col_status} and schema_test_cat_col_status: {schema_test_cat_col_status} ")
            logging.info("validated dataset schema for categorical columns")

            (
                schema_train_num_col_status,
                schema_test_num_col_status,
            ) = self.validate_is_numerical_column_exists()

            logging.info(f"schema_train_num_col_status: {schema_train_num_col_status} and schema_test_num_col_status: {schema_test_num_col_status} ")
            logging.info("validated dataset schema for numerical columns")

            drift_status = None

            if (
                schema_train_cat_col_status is True
                and schema_test_cat_col_status is True
                and schema_train_num_col_status is True
                and schema_test_num_col_status is True
                and schema_train_col_status is True
                and schema_test_col_status is True
                and drift is False
            ):
               logging.info("Dataset schema validation completed successfully")
               drift_status = True
            else:
                drift_status = False

            data_validation_artifacts = DataValidationArtifacts(
                data_drift_file_path = self.data_validation_config.DATA_DRIFT_FILE_PATH,
                validation_status=drift_status,
            )

            return data_validation_artifacts
        
        except Exception as e:
            raise shippingException(e, sys) from e