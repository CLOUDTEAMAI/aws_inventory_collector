import boto3
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import json
from cloudwatch_logic import *


# def executor():
#     for account in accounts:
#         for region in account_region:
#             ''' here will be the logic'''