from datetime import datetime
from os import listdir
import os
import sys
import re
import json
import shutil
import pandas as pd

from ExceptionLoggerAndUtils.exception import CustomException
from ExceptionLoggerAndUtils.logger import App_Logger

class Raw_Data_validation:
    """ This class shall be used for handling all the validation done on the Raw Training Data!!.
        Written By: Shivraj Shinde
        Version: 1.0
        Revisions: None
    """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'Schemas/schema_training.json'
        self.logger = App_Logger()


    def valuesFromSchema(self):
        """Method Name: valuesFromSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure: Raise Exception
            Written By: Shivraj Shinde
            Version: 1.0
            Revisions: None
            """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise CustomException(e,sys)

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """Method Name: manualRegexCreation
            Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
            This Regex is used to validate the filename of the training data.
            Output: Regex pattern
            On Failure: None

            Written By: Shivraj Shinde
            Version: 1.0
            Revisions: None
            """
        # sample file name: "creditCardFraud_021119920_010222.csv"

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the training csv files as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

            Written By: Shivraj Shinde
            Version: 1.0
            Revisions: None
                """


        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("RawAndValidatedData/RawData/" + filename, "RawAndValidatedData/ValidatedData/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("RawAndValidatedData/RawData/" + filename, "RawAndValidatedData/ValidatedData/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("RawAndValidatedData/RawData/" + filename, "RawAndValidatedData/ValidatedData/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("RawAndValidatedData/RawData/" + filename, "RawAndValidatedData/ValidatedData/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise CustomException(e,sys)

    def deleteExistingBadDataTrainingFolder(self):
        """
        Method Name: deleteExistingBadDataTrainingFolder
        Description: This method deletes the directory made to store the bad Data.
        Output: None
        On Failure: Error

        Written By: Shivraj Shinde
        Version: 1.0
        Revisions: None
            """

        try:
            path = 'RawAndValidatedData/ValidatedData/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')

                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s")
            file.close()
            raise CustomException(e,sys)


    def deleteExistingGoodDataTrainingFolder(self):
        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data
                          after loading the data in the table. Once the good files are
                          loaded in the DB,deleting the directory ensures space optimization.
            Output: None
            On Failure: OSError

            Written By: Shivraj Shinde
            Version: 1.0
            Revisions: None

                    """

        try:
            path = 'RawAndValidatedData/ValidatedData/'
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')


                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s")
            file.close()
            raise CustomException(e, sys)


    def createDirectoryForGoodBadRawData(self):

        """
          Method Name: createDirectoryForGoodBadRawData
          Description: This method creates directories to store the Good Data and Bad Data
                        after validating the training data.

          Output: None
          On Failure: OSError

           Written By: Shivraj Shinde
          Version: 1.0
          Revisions: None

                                              """

        try:
            path = os.path.join("RawAndValidatedData/ValidatedData/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("RawAndValidatedData/ValidatedData/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s")
            file.close()
            raise CustomException(e, sys)

