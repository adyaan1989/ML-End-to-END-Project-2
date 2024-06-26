from dataclasses import dataclass

#data Ingetion Artifacts
@dataclass
class DataIngestionArtifacts:
    train_data_file_path: str
    test_data_file_path: str
    
# Data Validation Artifacts
@dataclass
class DataValidationArtifacts:
    data_drift_file_path: str
    validation_status: bool


