# Database Connection Manager

## Overview

The **Database Connection Manager** is a Python script that allows you to connect to multiple MySQL databases and generate a JSON document containing information about the database connections. This script is particularly useful for managing connections to various databases in a dynamic environment.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Configuration](#configuration)
- [Excluded Databases](#excluded-databases)
- [Contributing](#contributing)
- [License](#license)

## Features

- Connect to multiple MySQL databases.
- Dynamically fetch database names from the MySQL server.
- Create a JSON document with detailed connection information.
- Exclude specific databases from the connection list.

## Getting Started

To get started with the Database Connection Manager, follow these steps:

1. **Clone the repository** to your local machine:

git clone https://github.com/your-username/DatabaseConnectionManager.git

markdown
Copy code

2. **Install the required Python packages** by running:

pip install pymysql

markdown
Copy code

3. **Configure the script** by setting the environment variables `LOYALTY_USER` and `LOYALTY_PWD` to your MySQL username and password.

4. **Run the script**:

python database_connection_manager.py

css
Copy code

5. The script will connect to the specified MySQL servers and generate a JSON document containing connection information.

## Usage

Once the script is executed, it will connect to the MySQL servers and create a JSON document. You can then use this JSON document for various purposes, such as monitoring and managing your database connections.

Example JSON document structure:

```json
[
 {
     "host": "prop1-db-write.prod.loyalty.krs.io",
     "database": "example_db_1"
 },
 {
     "host": "prop2-db-write.prod.loyalty.krs.io",
     "database": "example_db_2"
 },
 ...
]
Configuration
You can configure the script by setting the following environment variables:

LOYALTY_USER: Your MySQL username.
LOYALTY_PWD: Your MySQL password.
prop1_host: Hostname for the first MySQL server.
prop2_host: Hostname for the second MySQL server.
Excluded Databases
By default, the script excludes specific databases from the connection list to prevent unnecessary connections. You can modify the list of excluded databases in the script to suit your needs.

Contributing
Contributions to this project are welcome! If you have any improvements or feature suggestions, please open an issue or submit a pull request.
