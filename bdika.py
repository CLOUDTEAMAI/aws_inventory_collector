from psutil import Process
from os import getpid

# Function to get current memory usage
def get_memory_usage():
    process = Process(getpid())
    return process.memory_info().rss

# Measure memory usage before importing boto3
memory_before = get_memory_usage()
print(f"Memory usage before importing modules: {memory_before / (1024*1024):.2f} MB")

# Import boto3
import boto3
import concurrent.futures
import datetime
import pandas as pd
import json
import os
import threading
import psutil
# Measure memory usage after importing boto3
memory_after = get_memory_usage()
print(f"Memory usage after importing modules: {memory_after / (1024*1024):.2f} MB")
