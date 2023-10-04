import sys
import pandas as pd
from ExceptionLoggerAndUtils.utils import load_object
from ExceptionLoggerAndUtils.exception import CustomException


class PredictPipeline():
    def __init__(self):
        pass

    def precit(self,features):
        try:
            model_path = 'models/XGBoost0/XGBoost0.sav'
            preprocessor_path = 'models/KMeans/KMeans.sav'
            model = load_object(file_path=model_path)
            preprocessor = load_object(file_path=preprocessor_path)
            data_scaled = preprocessor.transform(features)
            preds = model.predict(data_scaled)
            return preds
        except Exception as e:
            raise CustomException(e,sys)


class CustomData:
    def __init__(self):
        pass

    def get_data_as_dataframe(self):
        df = pd.read_csv()
        return df