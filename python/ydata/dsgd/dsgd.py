__author__ = 'matthieu'

from abc import ABCMeta, abstractmethod

from sklearn.base import BaseEstimator, ClassifierMixin

__name__ = ["DSGDClassifier"]

class BaseDSGD(BaseEstimator):
   __metaclass__ = ABCMeta

   def __init__(self, D, alpha=0.1):
       self.D = D
       self.alpha = alpha

   @abstractmethod
   def fit(self, X, y):
        """
        Fit the model using X, y as training data
        :param X:
        :param y:
        :return:
        """

   @abstractmethod
   def predict(self, X):
        """
        Predict values using model
        :param X:
        :return:
        """

class DSGDClassifier(BaseDSGD, ClassifierMixin):
    def __init__(self, hsc, D=2 ** 20, alpha=0.1):
        super(DSGDClassifier, self).__init__(D, alpha)
        self.hsc = hsc
        self.api = self.hsc._jvm.ydata.api.python.PythonYdataAPI()
        self.model = None

    def fit(self, X, y):
        self.model = self.api.fitDSGD(X.data._jschema_rdd, y.data._jschema_rdd)

    def predict(self, X):
        from ..df import dataframe_from_schemardd
        import pyspark.sql

        rdd = self.api.predictDSGD(self.model, X.data._jschema_rdd)
        return dataframe_from_schemardd(self.hsc, pyspark.sql.SchemaRDD(rdd, self.hsc))

    def __repr__(self):
        return "DSGD"
