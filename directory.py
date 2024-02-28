import os
import json

# JSON data with services
base_dir = "/Users/eyalrainitz/Desktop/Code/Inventory/aws_services"

with open("servicesBoto3.json",'r') as file:
    json_file = json.load(file)
# Base directory where you want to create the service directories

# Ensure the base directory exists
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# Iterate through the services and create a directory for each
for service in json_file["services"]:
    service_dir = os.path.join(base_dir, service)
    if not os.path.exists(service_dir):
        os.makedirs(service_dir)
        print(f"Directory created for: {service}")
    else:
        print(f"Directory already exists for: {service}")
