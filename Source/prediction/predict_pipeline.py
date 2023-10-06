import sys
import pandas as pd
from ExceptionLoggerAndUtils.utils import load_object
from ExceptionLoggerAndUtils.exception import CustomException

class PredictPipeline():
    def __init__(self):
        self.model_path0 = 'models/XGBoost0/XGBoost0.sav'
        self.model_path1 = 'models/XGBoost1/XGBoost1.sav'
        self.model_path2 = 'models/XGBoost2/XGBoost2.sav'
        self.model_path3 = 'models/XGBoost3/XGBoost3.sav'
        self.preprocessor_path = 'models/proprocessor.pkl'
        self.kmeansPath = 'models/KMeans/KMeans.sav'

    def predit(self,features):
        try:
            kMeans = load_object(file_path=self.kmeansPath)
            clusterNumber =kMeans.predict(features)
            print('clusters',clusterNumber)

            print("finalPredX1")
            preprocessor = load_object(file_path=self.preprocessor_path)
            data_scaled = preprocessor.transform(features)
            print("finalPredX2")

            if clusterNumber == 0:
                model = load_object(file_path=self.model_path0)
                preds = model.predict(data_scaled)
                print("0")
            elif clusterNumber == 1:
                model = load_object(file_path=self.model_path1)
                preds = model.predict(data_scaled)
                print("1")
            elif clusterNumber == 2:
                model = load_object(file_path=self.model_path2)
                preds = model.predict(data_scaled)
                print("2")
            elif clusterNumber == 3:
                model = load_object(file_path=self.model_path3)
                preds = model.predict(data_scaled)
                print("3")

            return clusterNumber ,preds

        except Exception as e:
            raise CustomException(e,sys)



class CustomData:
    def __init__(self):
        pass

    def get_data_as_dataframe(self):
        df = pd.read_csv()
        return df

