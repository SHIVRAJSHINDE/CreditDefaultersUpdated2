from datetime import datetime
from os import listdir
import pandas
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
        Written By: Shivraj Shinde//Version: 1.0//Revisions: None
    """

    def __init__(self,path):
        self.goodDataPath = "RawAndValidatedData/ValidatedData/Good_Raw"
        self.Batch_Directory = path
        self.schema_path = 'Schemas/schema_training.json'
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """ Method Name : valuesFromSchema
            Written By  : Shivraj Shinde//Version: 1.0//Revisions: None
            Description : This method extracts all the relevant information from the pre-defined "Schema" file.
            Output      : LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure  : Raise Exception
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
        """ Method Name :manualRegexCreation
            Written By  : Shivraj Shinde//Version: 1.0//Revisions: None
            Description :This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                            This Regex is used to validate the filename of the training data.
            Output      :Regex pattern
            On Failure  :None
            """
        # sample file name: "creditCardFraud_021119920_010222.csv"

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """ Method Name : validationFileNameRaw
            Written By  : Shivraj Shinde//Version: 1.0//Revisions: None
            Description : This function validates the name of the training csv files as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output      : None
            On Failure  : Exception
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
        """     Method Name : deleteExistingBadDataTrainingFolder
                Written By  : Shivraj Shinde//Version: 1.0//Revisions: None
                Description : This method deletes the directory made to store the bad Data.
                Output      : None
                On Failure  : Error
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



    def validateColumnLength(self,NumberofColumns):
        """
          Method Name: validateColumnLength
          Description: This function validates the number of columns in the csv files.
                       It is should be same as given in the schema file.
                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                       If the column number matches, file is kept in Good Raw Data for processing.
                      The csv file is missing the first column name, this function changes the missing name to "creditCardFraud".
          Output: None
          On Failure: Exception

        Written By: Shivraj Shinde
        Version: 1.0
        Revisions: None

        """
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")

            for file in listdir('RawAndValidatedData/ValidatedData/Good_Raw/'):
                csv = pd.read_csv("RawAndValidatedData/ValidatedData/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("RawAndValidatedData/ValidatedData/Good_Raw/" + file, "RawAndValidatedData/ValidatedData/Bad_Raw/")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise CustomException(e, sys)


    def validateMissingValuesInWholeColumn(self):
        """
        Method Name: validateMissingValuesInWholeColumn
        Description: This function validates if any column in the csv file has all values missing.
                   If all the values are missing, the file is not suitable for processing.
                   SUch files are moved to bad raw data.
        Output: None
        On Failure: Exception

        Written By: Shivraj Shinde
        Version: 1.0
        Revisions: None

        """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir('RawAndValidatedData/ValidatedData/Good_Raw/'):
                csv = pd.read_csv("RawAndValidatedData/ValidatedData/Good_Raw/" + file)
                count = 0

                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("RawAndValidatedData/ValidatedData/Good_Raw/" + file,
                                    "RawAndValidatedData/ValidatedData/Bad_Raw")
                        self.logger.log(f,"Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("RawAndValidatedData/ValidatedData/Good_Raw/" + file, index=None, header=True)

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise CustomException(e, sys)
        f.close()


    def replaceMissingWithNull(self):
        """
        Method Name: replaceMissingWithNull
        Written By: Shivraj Shinde//Version: 1.0//Revisions: None
        Description: This method replaces the missing values in columns with "NULL" to
                    store in the table. We are using substring in the first column to
                    keep only "Integer" data for ease up the loading.
                    This column is anyways going to be removed during training.
        """

        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')
        try:
           onlyfiles = [f for f in listdir(self.goodDataPath)]
           for file in onlyfiles:
                data = pandas.read_csv(self.goodDataPath + "/" + file)
                data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: Quotes added successfully!!" % file)
           #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
           #log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "Data Transformation failed because:: %s" % e + "\n")
            log_file.close()
            raise CustomException(e, sys)
        log_file.close()

    def moveBadFilesToArchiveBad(self):
        """
        Method Name: moveBadFilesToArchiveBad
        Written By: Shivraj Shinde//Version: 1.0//Revisions: None

        Description: This method deletes the directory made  to store the Bad Data
                      after moving the data in an archive folder. We archive the bad
                      files to send them back to the client for invalid data issue.
        Output: None
        On Failure: OSError


        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'RawAndValidatedData/ValidatedData/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

