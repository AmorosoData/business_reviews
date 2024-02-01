import os
import sys
import pymysql
import json
from datetime import datetime
import pandas as pd  # Import pandas

def get_config():
    """
    Fetch configuration settings from environment variables.
    """
    try:
        config = {
            'loy_user': os.environ['LOYALTY_USER'],
            'loy_password': os.environ['LOYALTY_PWD'],
            'prop1_host': 'prop1-db-write.prod.loyalty.krs.io',
            'prop2_host': 'prop2-db-write.prod.loyalty.krs.io',
            'prop1_dbs': [],  # Will be populated dynamically
            'prop2_dbs': []  # Will be populated dynamically
        }
    except KeyError as e:
        print(f'Missing Environmental Variable: {e}')
        sys.exit(9)

    return config

def log_error(message):
    """
    Log an error message to a file.
    """
    with open("database_connection_errors.log", "a") as log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"{timestamp} - {message}\n")

def connect_database(host, user, password, db_name):
    """
    Attempt to connect to the MySQL Database and return the connection.
    """
    try:
        connection = pymysql.connect(host=host, user=user, password=password, db=db_name)
        print(f"Connected to MySQL Database {db_name}")
        return connection
    except pymysql.MySQLError as e:
        log_error(f"Error connecting to MySQL Database {db_name}: {e}")
        return None

# Modify the code to dynamically fetch database names
def get_database_names(host, user, password):
    try:
        connection = pymysql.connect(host=host, user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        database_names = [row[0] for row in cursor.fetchall()]
        connection.close()
        return database_names
    except pymysql.MySQLError as e:
        log_error(f"Error fetching database names: {e}")
        return []

# Define the list of excluded database names
excluded_databases = [
    'achaloha', 'achcampau', 'achcedars', 'achfreddy', 'achklamath', 'achmattjeff', 'achmenominee', 'achrainbow', 'achsilver_reef', 'achtylerpetro', 'beta_kickbackpoints_com', 'beta_kickbacksystems_com', 'billing', 'billing_peakrewards', 'brandon_test', 'cas', 'els', 'information_schema', 'kickbacksystems_com', 'loyalty_performance', 'loyempty', 'mattjeff', 'monitoring', 'mysql', 'nl_kickbackpoints_com', 'percona', 'performance_schema', 'portal', 'portal_advancedlandholdings', 'portal_airportplaza', 'portal_alltown', 'portal_alon', 'portal_alon_old', 'portal_arrow', 'portal_bigapplerewards', 'portal_blueharbor', 'portal_bonneaubonus', 'portal_brandwagon', 'portal_burkerewards', 'portal_circle11', 'portal_colvillerewards', 'portal_corriganmyrewards', 'portal_darusa', 'portal_extramile', 'portal_fastrewards', 'portal_faststop', 'portal_faststopperks', 'portal_fleetwayrewards', 'portal_fuelmarket', 'portal_gasking', 'portal_gmreward', 'portal_goodoil', 'portal_greenevillequickstop', 'portal_jacksons', 'portal_jimdandy', 'portal_jrfoodmart', 'portal_maccardrewards', 'portal_marineview', 'portal_meyeroilcompany', 'portal_millers', 'portal_mrgas', 'portal_nacstcb', 'portal_ortons', 'portal_peakrewards', 'portal_qrewards', 'portal_qualitydairy', 'portal_redwoodoil', 'portal_roadsiderewards', 'portal_roamnrewards', 'portal_rocrewards', 'portal_sevenstars', 'portal_sheetzrewards', 'portal_shepherdsrewards', 'portal_silverreefcasino', 'portal_sprintmart', 'portal_starexpressperks', 'portal_sullys', 'portal_timesreward', 'portal_unitedpacemployee', 'portal_vitusadvantage', 'portalaloha', 'portalalon', 'portalalon_old', 'portalarrow', 'portalautopia', 'portalbiindigen', 'portalbishop', 'portalbjtobacco', 'portalbobbyandsteves', 'portalbreadandbutter', 'portalcalispel', 'portalcampau', 'portalcandmsupply', 'portalcanyonferry', 'portalcdatribe', 'portalcedars', 'portalcedarsloygift', 'portalchehalis', 'portalchic', 'portalcirclenmarket', 'portalcolville', 'portalcomitesmartclub', 'portalcopdemo', 'portaldiamondmountain', 'portaldroppoints', 'portaldukes', 'portaldvorewards', 'portalempty', 'portalexpressmart', 'portalexpresspointspass', 'portalezmoney', 'portalfasrewards', 'portalfastbreak', 'portalfiscal', 'portalflashmarkets', 'portalflyingk', 'portalfoodnfun', 'portalfortheroad', 'portalfreddy', 'portalgasngo', 'portalgee_cees', 'portalgetngo', 'portalhandymart', 'portalharts', 'portalhillbilly', 'portalhoneymoney', 'portaljalou', 'portaljfj', 'portaljohnson', 'portalkalispel', 'portalkboil', 'portalkiddjonesgift', 'portalklamath', 'portalkwiktrip', 'portalleerjak', 'portallittlestar', 'portallumminontribal', 'portalmaccardrewards', 'portalmarineview', 'portalmattjeff', 'portalmenominee', 'portalmeyeroilcompany', 'portalminitstop', 'portalniceneasy', 'portalnoco', 'portalnocorewards', 'portalnte', 'portalobo', 'portaloncuepumpstart', 'portalosagecasino', 'portalparkers', 'portalphilipmorris', 'portalpitstop', 'portalpmgairportplaza', 'portalpocketmoney', 'portalporter', 'portalportmadison', 'portalpromostogo', 'portalpuyallup', 'portalquickstop', 'portalrainbow', 'portalrattlers', 'portalrickers', 'portalridleys', 'portalrmroachoil', 'portalroyalcash', 'portalrt66foodandfuel', 'portalschraderoil', 'portalshorewood', 'portalsignals', 'portalsilver_reef', 'portalspiritrewards', 'portalstopngo', 'portalstripes', 'portalsunnyrewards', 'portalsunnyside', 'portalswansons', 'portalswinomish', 'portalterribleherbst', 'portaltexasbest', 'portalthorntons', 'portaltowles', 'portaltradingpostrewards', 'portaltreehouse', 'portaltribaltrails', 'portaltulalip', 'portaltylerpetro', 'portaluncle_neals', 'portaluncles', 'portalvalley', 'portalvalleymart', 'portalwoodsheds', 'portalworld_rewards', 'portalzstop', 'proxydemo', 'put', 'reports', 'resources', 'signature', 'store_kickbacksystems_com', 'sys', 'test', 'tmp', 'upc', 'user', 'www_kickbackpoints_com_stale_moved_to_coalition_server'
    ]

import pandas as pd  # Import pandas

# ... (Previous code remains the same)

def main():
    config = get_config()

    # Fetch database names for prop1
    prop1_database_names = get_database_names(config['prop1_host'], config['loy_user'], config['loy_password'])
    config['prop1_dbs'] = [db_name for db_name in prop1_database_names if db_name not in excluded_databases]

    # Fetch database names for prop2
    prop2_database_names = get_database_names(config['prop2_host'], config['loy_user'], config['loy_password'])
    config['prop2_dbs'] = [db_name for db_name in prop2_database_names if db_name not in excluded_databases]

    connection_info = []

    # Connect to prop1 databases
    for db_name in config['prop1_dbs']:
        connection = connect_database(config['prop1_host'], config['loy_user'], config['loy_password'], db_name)
        if connection:
            connection_info.append({
                'host': config['prop1_host'],
                'user': config['loy_user'],
                'db_name': db_name,
                'name': '',  # Manually populate the name
                'include_locations': [],  # Manually populate the include_locations
                'exclude_locations': []   # Manually populate the exclude_locations
            })

            # Execute the query and save results to a CSV
            execute_and_save_query(connection, db_name)
            
            connection.close()  # Close the connection here

    # Connect to prop2 databases
    for db_name in config['prop2_dbs']:
        connection = connect_database(config['prop2_host'], config['loy_user'], config['loy_password'], db_name)
        if connection:
            connection_info.append({
                'host': config['prop2_host'],
                'user': config['loy_user'],
                'db_name': db_name,
                'name': '',  # Manually populate the name
                'include_locations': [],  # Manually populate the include_locations
                'exclude_locations': []   # Manually populate the exclude_locations
            })

            # Execute the query and save results to a CSV
            execute_and_save_query(connection, db_name)
            
            connection.close()  # Close the connection here

    # Define the path to the JSON file in your Downloads folder
    json_file_path = os.path.expanduser("~/Downloads/database_connections.json")

    # Write the connection information to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(connection_info, json_file, indent=4)


REQUIRED_TABLES = ['ppccustomers', 'dailypointstrx']

def execute_and_save_query(connection, db_name):
    try:
        # Check if the required tables exist in the database
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        existing_tables = [row[0] for row in cursor.fetchall()]

        missing_tables = [table for table in REQUIRED_TABLES if table not in existing_tables]

        if missing_tables:
            print(f"{db_name} is not a valid customer. Missing tables: {', '.join(missing_tables)}")
            return

        query = """
        SELECT
            CONCAT(SUBSTRING(d.TrxDate, 1, 4), '-', SUBSTRING(d.TrxDate, 5, 2)) AS 'YearMonth',
            p.custid,
            p.name,
            p.storenumber,
            p.active,
            COUNT(DISTINCT d.CardNumber) AS unique_cards_used,    
            COUNT(d.TrxNumber) AS 'TransactionCount',
            SUM(d.Quantity) AS 'TotalQuantity'
        FROM
            ppccustomers p
        LEFT JOIN
            dailypointstrx d ON p.custid = d.LocationID
        GROUP BY
            YearMonth,
            p.custid,
            p.name,
            p.storenumber,
            p.active;
        """

        # Execute the query and fetch the results into a DataFrame
        df = pd.read_sql(query, connection)

        # Create a CSV file with the results
        csv_filename = f"{db_name}_query_results.csv"
        df.to_csv(csv_filename, index=False)

        print(f"Query results for database '{db_name}' saved to '{csv_filename}'")

    except pymysql.MySQLError as e:
        log_error(f"Error executing query for database '{db_name}': {e}")


if __name__ == "__main__":
    main()
