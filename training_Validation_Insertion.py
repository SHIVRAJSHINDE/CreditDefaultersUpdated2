import sys
from Source.dataValidation.rawDataValidation import Raw_Data_validation
from Source.dataIngestionAndSplitting.dataIngestionAndSplitting import dBOperation
from ExceptionLoggerAndUtils.exception import CustomException
from ExceptionLoggerAndUtils.logger import App_Logger

import os

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dbOperation = dBOperation()

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

            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.raw_data.replaceMissingWithNull()
            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dbOperation.createTableDb('Training', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")


            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dbOperation.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")

            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")


            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")

            self.file_object.close()

        except Exception as e:
            raise CustomException(e,sys)


if __name__ == "__main__":
    obj = train_validation("D:\MachineLearningProjects\PROJECT\creditDaultersUpdated\RawAndValidatedData\RawData")
    #obj = train_validation("D:\MachineLearningProjects\PROJECT\WafersFaultDetection\Training_Batch_Files")
    obj.train_validation()






