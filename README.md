# aws_inventory_collector

### Prerequisites
- AWS CLI installed and configured (see [AWS CLI Installation](https://aws.amazon.com/cli/)).
- Python 3.9+ installed.
- Access to AWS accounts with necessary permissions.

Here is an overview of our project architecture:

![Project Architecture Diagram](Readme/collector.svg?raw=true)

The diagram above illustrates the flow of data through our application, showing the interaction between different services.



### Components Description

- **Main.py**: Central script that orchestrates the data collection processes. It's extendable for additional functionalities.
- **Collector**: This module contains all the data collection logic for various AWS services in parallel.
- **DB**: Manages all interactions with databases, ensuring data integrity and efficiency.
- **Utils**: Provides common utilities and helper functions used across the project.
- **Outputs**: Handles the generation and storage of output files, primarily in Parquet format, for further analysis.

## Setup and Operation
1. **Clone the Repository**: Clone this repository to your local machine or server.
2. **Install Dependencies**: Run `pip install -r requirements.txt` to install required Python packages.
3. **Configure AWS CLI**: Ensure AWS CLI is configured with access to the necessary AWS accounts.
4. **Run the Collector**: Execute `python main.py` to start the collection process. Check the logs for progress and results.



## Performance
1. **change worker** will effect the runinng time for faster running increase the worker count and if suffer from over utillization change the worker count for less then default