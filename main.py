import pyodbc
import json
from datetime import datetime
import configparser

# Function to read configuration from config.ini file
def read_config(filename='config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config['MSSQL']

# Function to establish connection to MSSQL server
def connect_to_mssql(config):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};UID={config['username']};PWD={config['password']}"
    return pyodbc.connect(conn_str)

# Function to execute SQL query and return results as JSON
def execute_query_to_json(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return data

# Main function
def main():
    # Read configuration from config.ini
    config = read_config()

    # Connect to MSSQL server
    try:
        connection = connect_to_mssql(config)
        print("Connected to MSSQL Server")
    except Exception as e:
        print("Error connecting to MSSQL Server:", e)
        return

    # Get SQL query and columns from config
    query = config['query']
    columns = [col.strip() for col in config['columns'].split(',')]

    # Execute query and get data
    try:
        data = execute_query_to_json(connection, query)
        print("Query executed successfully")
    except Exception as e:
        print("Error executing query:", e)
        connection.close()
        return

    # Close connection
    connection.close()

    # Generate timestamp for filename
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # Save data to JSON file with timestamp in filename
    filename = f"data_{timestamp}.json"
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Data saved to {filename}")

if __name__ == "__main__":
    main()
