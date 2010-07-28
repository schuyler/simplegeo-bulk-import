import simplegeo
import sys, os
from pprint import pprint

layer, id = sys.argv[1:3]
token = os.environ["SIMPLEGEO_TOKEN"]
secret = os.environ["SIMPLEGEO_SECRET"]
client = simplegeo.Client(token, secret)
result = client.get_record(layer, id)
pprint(result)
