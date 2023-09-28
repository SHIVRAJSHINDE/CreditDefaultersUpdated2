from datetime import datetime
import sys
from Source.DataValidationAndIngestion.rawDataValidation import Raw_Data_validation
''''from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform'''

from ExceptionLoggerAndUtils.exception import CustomException
from ExceptionLoggerAndUtils.logger import App_Logger

import os

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)


        self.cwd=os.getcwd()
        self.file_object = open(self.cwd+'Training_Main_Log.txt', 'a+')
        self.log_writer = App_Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files for Training')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()

            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            print(regex)

            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)


            '''            # validating column length in the file
                        self.raw_data.validateColumnLength(noofcolumns)
            '''


        except Exception as e:
            raise CustomException(e,sys)


if __name__ == "__main__":
    obj = train_validation("D:\MachineLearningProjects\PROJECT\creditDaultersUpdated\RawAndValidatedData\RawData")
    #obj = train_validation("D:\MachineLearningProjects\PROJECT\WafersFaultDetection\Training_Batch_Files")
    obj.train_validation()






