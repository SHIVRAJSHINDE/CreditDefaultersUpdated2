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
from Source.Transformation.dataTransformation import classePreprocessing
from Source.Transformation.clustering import KMeansClustering
from Source.Training import tuner
from Source.dataIngestionAndSplitting.dataIngestionAndSplitting import dBOperation
from Source.Transformation import preprocessing
from ExceptionLoggerAndUtils import file_methods

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
        self.transformation = classePreprocessing()
        self.log_writer = App_Logger()
        self.preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
        self.model_finder = tuner.Model_Finder(self.file_object, self.log_writer)  # object initialization

    def methodTransformation(self):
        self.log_writer.log(self.file_object, "Extracting csv file from table")
        # export data in table to csvfile

        X,y = self.dbOperation.createInputAndOutputDataset()
        kmeans = KMeansClustering(self.file_object, self.log_writer)  # object initialization.
        number_of_clusters = kmeans.elbow_plot(X)  # using the elbow plot to find the number of optimum clusters
        print(number_of_clusters)
        X = kmeans.create_clusters(X, number_of_clusters)

        # create a new column in the dataset consisting of the corresponding cluster assignments.
        X['Labels'] = y

        # getting the unique clusters from our dataset
        list_of_clusters = X['Cluster'].unique()
        print(list_of_clusters)

        """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""
        self.file_op = file_methods.File_Operation(self.file_object, self.log_writer)


        for i in list_of_clusters:
            cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster
            cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
            cluster_label = cluster_data['Labels']
            X_train, X_test, y_train, y_test = self.dbOperation.dataSplittingToTrainAndTest(X=cluster_features,y=cluster_label)

            '''train_x = self.preprocessor.scale_numerical_columns(X_train)
            test_x = self.preprocessor.scale_numerical_columns(X_test)
            '''
            pipe = self.transformation.methodPreprocessing()
            train_x = pipe.fit_transform(X_train)
            test_x = pipe.transform(X_test)

            model_finder=tuner.Model_Finder(self.file_object,self.log_writer) # object initialization

            #getting the best model for each of the clusters
            best_model_name,best_model=model_finder.get_best_model(train_x,y_train,test_x,y_test)
            print(best_model_name)
            print(best_model)

            #saving the best model to the directory.
            file_op = file_methods.File_Operation(self.file_object,self.log_writer)
            save_model=file_op.save_model(best_model,best_model_name+str(i))

        # logging the successful Training
        self.log_writer.log(self.file_object, 'Successful End of Training')
        self.file_object.close()


        '''print(y_train)
        print(y_test)'''



if __name__ == "__main__":
    obj = classTransformation()
    #obj = train_validation("D:\MachineLearningProjects\PROJECT\WafersFaultDetection\Training_Batch_Files")
    obj.methodTransformation()
