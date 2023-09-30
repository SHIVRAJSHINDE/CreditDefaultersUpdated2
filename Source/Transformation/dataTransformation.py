
from ExceptionLoggerAndUtils.logger import App_Logger
from ExceptionLoggerAndUtils.exception import CustomException
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import make_pipeline,Pipeline


class classePreprocessing:
    """
    Written By: Shivraj Shinde//Version: 1.0//Revisions: None
    This class shall be used for transformation and preprocessing like handeling Missing Value,scaling.
    """

    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "RawAndValidatedData/ValidatedData/Bad_Raw"
        self.goodFilePath = "RawAndValidatedData/ValidatedData/Good_Raw"
        self.logger = App_Logger()

    def methodPreprocessing(self):
        trf1 = ColumnTransformer([
            ('scale', MinMaxScaler(), slice(0, 24))
        ])

        pipe = make_pipeline(trf1)
        return pipe


