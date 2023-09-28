"""
Here we start data pre processing and then Clustering.
Written By: Shivraj Shinde//Version: 1.0//Revisions: None

"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
'''from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods'''
from Source.dataIngestionAndSplitting.dataIngestionAndSplitting import dBOperation

from ExceptionLoggerAndUtils.exception import CustomException
from ExceptionLoggerAndUtils.logger import App_Logger
import numpy as np
import pandas as pd

#Creating the common Logging object


class classTransformation:

    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
        self.dbOperation = dBOperation()


    def methodTransformation(self):
        self.log_writer.log(self.file_object, "Extracting csv file from table")
        # export data in table to csvfile
        self.dbOperation.selectingDatafromtableintocsv('Training')
        X_train, X_test, y_train, y_test = self.dbOperation.dataSplittingToTrainAndTest()


        '''print(X_train)
        print(X_test)
        print(y_train)
        print(y_test)'''




if __name__ == "__main__":
    obj = classTransformation()
    #obj = train_validation("D:\MachineLearningProjects\PROJECT\WafersFaultDetection\Training_Batch_Files")
    obj.methodTransformation()
