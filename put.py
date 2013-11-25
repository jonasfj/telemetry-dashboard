from boto.s3 import connect_to_region as s3_connect
from boto.s3.key import Key
import sys

s3 = s3_connect('us-west-2', ...)
b = s3.get_bucket('dashboard-mango-aggregates', validate = False)
k = Key(b)
k.key = sys.argv[1]
k.set_contents_from_filename(sys.argv[1])