import sys
from shipment.exception import shippingException
from shipment.logger import logging
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.artifacts_entity import (
    DataIngestionArtifacts,
    DataValidationArtifacts,
    DataTransformationArtifacts,
)
from shipment.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
)
from shipment.components.data_ingestion import DataIngestion
from shipment.components.data_validation import DataValidation
from shipment.components.data_transformation import DataTransformation


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()

        self.mongo_op = MongoDBOperation()

    def start_data_ingestion(self) -> DataIngestionArtifacts:
        logging.info("Entered start_data_ingestion method.")
        try:
            logging.info("Getting data from MongoDB.")
            data_ingestion = DataIngestion(
                data_ingestion_config=self.data_ingestion_config,
                mongo_op=self.mongo_op
            )
            data_ingestion_artifacts = data_ingestion.initiate_data_ingestion()
            logging.info("Data ingestion completed.")
            return data_ingestion_artifacts
        
        except Exception as e:
            raise shippingException(f"Error during data ingestion: {e}")

    def start_data_validation(self, data_ingestion_artifacts: DataIngestionArtifacts) -> DataValidationArtifacts:
        logging.info("Entered start_data_validation method.")
        try:
            data_validation = DataValidation(
                data_ingestion_artifacts=data_ingestion_artifacts,
                data_validation_config=self.data_validation_config
            )
            data_validation_artifacts = data_validation.initiate_data_validation()
            logging.info("Data validation completed.")
            return data_validation_artifacts
        
        except Exception as e:
            raise shippingException(f"Error during data validation: {e}")

    
    def start_data_transformation(
            self, data_ingestion_artifacts: DataIngestionArtifacts
     
    ) -> DataTransformationArtifacts:
        logging.info("Entered the start_data_transformation method of  TrainPipeline class")

        try:
            data_transformation = DataTransformation(
                data_ingestion_artifacts=data_ingestion_artifacts,
                data_transformation_config=self.data_transformation_config,
            )
            data_transformation_artifacts = (
                data_transformation.initiate_data_transformation()
            )
            logging.info("Exited to start_data_transformation method of TrainPipeline class")
            return data_transformation_artifacts
        
        except Exception as e:
            raise shippingException(e, sys) from e

 
    
    def run_Pipeline(self) -> None:
        logging.info("Entered run_pipeline method.")
        try:
            data_ingestion_artifacts = self.start_data_ingestion()
            data_validation_artifacts = self.start_data_validation(data_ingestion_artifacts)
            data_transformation_artifact = self.start_data_transformation(
                data_ingestion_artifacts=data_ingestion_artifacts)
                
            logging.info("Pipeline execution completed.")
      
        except shippingException as se:
            logging.error(f"ShippingException occurred: {se}")
            raise  # Re-raise the ShippingException to propagate it up

        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            raise  # Re-raise any other exceptions with the full traceback



if __name__ == "__main__":
    pipeline = TrainPipeline()
    pipeline.run_Pipeline()
