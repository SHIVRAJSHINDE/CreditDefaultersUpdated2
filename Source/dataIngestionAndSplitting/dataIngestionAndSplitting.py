import shutil
import sqlite3
from datetime import datetime
from os import listdir
import sys
import pandas as pd
import os
import csv
from ExceptionLoggerAndUtils.logger import App_Logger
from ExceptionLoggerAndUtils.exception import CustomException
import mysql.connector as connection
from sklearn.model_selection import train_test_split


class dBOperation:
    """
      This class shall be used for handling all the SQL operations.
      Written By: Shivraj Shinde
      Version: 1.0
      Revisions: None
      """

    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "RawAndValidatedData/ValidatedData/Bad_Raw"
        self.goodFilePath = "RawAndValidatedData/ValidatedData/Good_Raw"
        self.logger = App_Logger()


    def dataBaseConnection(self,DatabaseName):
        """
        Method Name: dataBaseConnection
        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
        Output: Connection to the DB
        On Failure: Raise ConnectionError

        Written By: Shivraj Shinde
        Version: 1.0
        Revisions: None

        """
        try:
            conn = connection.connect(host="localhost", user="root", passwd="Ashiv@0511", database=DatabaseName)

            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()

        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError

        return conn

    def createTableDb(self,DatabaseName,column_names):
        """
        Method Name: createTableDb
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception

        Written By: Shivraj Shinde // Version: 1.0 // Revisions: None
        """

        try:
            conn = self.dataBaseConnection(DatabaseName)
            print("Database created")

            c = conn.cursor()
            # c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            table_name = "Good_Raw_Data"
            keyOne = 'abc'
            typeONE = "varchar(255)"
            drop_table_sql = f"DROP TABLE IF exists {table_name};"
            c.execute(drop_table_sql)
            print()

            create_table_sql = f"CREATE TABLE {table_name} ({keyOne} {typeONE});"
            print(create_table_sql)
            c.execute(create_table_sql)

            for key, col_type in column_names.items():
                # if key=='Wafer':
                #    col_type = "varchar(255)"
                key = "`" + key + "`"

                alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN {key} {col_type};"
                c.execute(alter_table_sql)

            drop_col = f"ALTER TABLE {table_name} DROP COLUMN {keyOne};"
            c.execute(drop_col)

            conn.close()

            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e

    def insertIntoTableGoodData(self,Database):
        """
        Method Name: insertIntoTableGoodData
        Description: This method inserts the Good data files from the Good_Raw folder into the
                    above created table.
        Output: None
        On Failure: Raise Exception

        Written By: iNeuron Intelligence//Version: 1.0//Revisions: None

        """

        conn = self.dataBaseConnection(Database)
        c = conn.cursor()

        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        table_name = "Good_Raw_Data"

        # c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")


        for file in onlyfiles:
            try:
                with open(goodFilePath + '/' + file, 'r') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    next(csv_reader)  # Skip the header row if present

                    for row in csv_reader:
                            try:
                                insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(row))})"
                                c.execute(insert_query, tuple(row))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()

                            except Exception as e:
                                raise e


            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise CustomException(e, sys)
        conn.close()
        log_file.close()



    def selectingDatafromtableintocsv(self,Database):

        """
        Method Name: selectingDatafromtableintocsv
        Written By: iNeuron Intelligence//Version: 1.0//Revisions: None

        Description: This method exports the data in GoodData table as a CSV file. in a given location.
                    above created .
        Output: None
        On Failure: Raise Exception
        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()

            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)
            print(csvFile)
            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()

    def createInputAndOutputDataset(self):
        df = pd.read_csv("Training_FileFromDB/InputFile.csv")
        X = df.iloc[:,:-1]
        y = df['default payment next month']
        return (X,y)

    def dataSplittingToTrainAndTest(self,X,y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.33, random_state = 355)

        return (X_train, X_test, y_train, y_test)

