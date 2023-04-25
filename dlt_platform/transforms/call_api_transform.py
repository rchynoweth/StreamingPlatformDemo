import requests
from pyspark.sql.functions import udf

class CallApiTransform():
  """
  Class used for transformations requiring an API call. 
  """

  def __init__(self):
    self.api_udf = udf(self.call_rest_api)

  def call_rest_api(self, string_column_value, url="https://cat-fact.herokuapp.com/facts/"):
    """ Example Rest API call to open API from Postman """
    assert type(string_column_value) == str
    # string value is testing the ability to pass in column values
    # public REST API from PostMan: https://documenter.getpostman.com/view/8854915/Szf7znEe
    response = requests.get(url)
    return response.text
