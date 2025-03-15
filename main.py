import os
import sys
import time
import pandas as pd
import uuid
import requests
from logger_custom import LoggerCustom
from dotenv import load_dotenv

from constants import SUPPORTED_FIELD_TYPES, FIBERY_FIELD_GENERAL

# Set the logger
log_file_path = 'log/main.log'
LEVEL_LOGGER = "INFO"
logger = LoggerCustom(log_file_path, LEVEL_LOGGER).get_logger()

# Добавляем обработчик для вывода в консоль
load_dotenv()

def time_countdown(seconds):
    while seconds > 0:
        # Return the remaining time
        sys.stdout.write(f'\rThere are left {seconds} seconds left until the next stage')
        sys.stdout.flush()
        time.sleep(1)
        seconds -= 1
    logger.info('\rTime has passed!                                                                   ')  

class FiberyAgent:
    def __init__(self, url, token):
        self.url = url
        self.token = token

    def send_data(self, data):
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
        }
        response = requests.post(self.url, headers=headers, json=data)
       
        if response.status_code != 200:
            logger.error(f"Error {response.status_code}: {response.text}")
            return None
        
        return response
    
    def get_schema(self):
        data = [{ "command": "fibery.schema/query" }]
        data_schema = self.send_data(data)

        if data_schema.status_code != 200:
            logger.error(f"Failed to get schema: {data_schema.text}")
            exit(1)
        
        return data_schema
        
    def create_database(self, app_name, database_name, fields):
        # List to store information about fields
        fibery_fields = []

        # Processing each custom field
        for count, (field_name, field_type) in enumerate(fields.items()):
            # Checking support for the specified field type
            if field_type not in SUPPORTED_FIELD_TYPES:
                raise ValueError(f"Тип '{field_type}' не поддерживается. "
                                f"Допустимые типы: {', '.join(SUPPORTED_FIELD_TYPES.keys())}.")

            # If this is the first field, set it as the primary field
            if count == 0:
                fibery_field ={
                        "fibery/name": f"{database_name}/{field_name}",
                        "fibery/type": "fibery/text",
                        "fibery/meta": {
                            "fibery/secured?": False,
                            "ui/title?": True
                        }
                        }
                fibery_fields.append(fibery_field)
                # Adding system fields
                fibery_fields.extend(FIBERY_FIELD_GENERAL)
            else:
                # Forming the structure for the current field
                fibery_field = {
                    "fibery/name": f"{database_name}/{field_name}",
                    "fibery/type": SUPPORTED_FIELD_TYPES[field_type],
                    "fibery/meta": {
                        "fibery/secured?": False
                    }
                }

            # Adding the field to the list
            fibery_fields.append(fibery_field)

        # General data structure for schema creation
        general_data = [
            {
                "command": "fibery.schema/batch",
                "args": {
                    "commands": [
                        {
                            "command": "schema.type/create",
                            "args": {
                                "fibery/name": f"{app_name}/{database_name}",
                                "fibery/meta": {
                                    "fibery/domain?": True,
                                    "fibery/secured?": True,
                                    "ui/color": "#F7D130"
                                },
                                "fibery/fields": fibery_fields
                            }
                        }
                    ]
                }
            }
        ]
        
        response_create_database = self.send_data(general_data)

        if response_create_database.status_code != 200:
            logger.error(f"Failed to create database: {response_create_database.text}")
            exit(1)
        else:
            # Parse the JSON content
            response_json = response_create_database.json()
            # logger.debug(response_json)
            # Check if the response is a list and has at least one element
            if isinstance(response_json, list) and len(response_json) > 0:
                # Access the 'success' key in the first element
                success = response_json[0].get("success", None)
                if not success:
                    false_message = response_json[0].get("result", {}).get("message", None)
                    if 'database already exists' in false_message :
                        logger.warning(f"Database {database_name} was exist: {false_message}")
                        success = True
                    else:
                        logger.error(f'Database "{database_name}"  did not create in app "{app_name}". Check the availability of an application with a name "{app_name}". Creat app in manual mode. Error: {false_message}')
                        success = False
                        exit(1)
                else:
                    logger.success(f'Database "{database_name}" was created in app "{app_name}". Status: {success}')
                    success = True
                
            else:
                logger.error("Unexpected response format or empty response.")

        return success
    
    def get_fields(self, app_name, database_name):
        data = [{"command": "fibery.schema/query", "database": f"{app_name}/{database_name}"}]
        response = self.send_data(data)
        data_fields = {}
        if response.status_code != 200:
            logger.error(f"Failed to get fields: {response.text}")
            exit(1)
        else:
            # Parse the JSON content
            response_json = response.json()[0].get("result", {}).get("fibery/types", [])
            for entity_type in response_json:
                fibery_name = entity_type['fibery/name']
                if fibery_name == f'{app_name}/{database_name}':
                    raw_fields = entity_type['fibery/fields']
                    filtered_fields = [field for field in raw_fields
                                    if field['fibery/name'].startswith(f'{app_name}/') or field['fibery/name'].startswith(f'{database_name}/')]
                    # logger.debug(filtered_fields)
                    for field in filtered_fields:
                        # logger.debug(f"Field Name: {field['fibery/name']}, Field Type: {field['fibery/type']}")
                        data_fields.update({field['fibery/name'].split('/')[-1].strip(): field['fibery/type'].split('/')[-1].strip()})

        return data_fields
    
    def delete_database(self, app_name, database_name):
        # Full entity type name
        entity_type = f'{app_name}/{database_name}'

        # Corrected payload for deleting the database
        delete_payload = [
            {
                "command": "fibery.schema/batch",
                "args": {
                    "commands": [
                        {
                            "command": "schema.type/delete",
                            "args": {
                                "name": entity_type,
                                "delete-entities?": True,
                                "delete-related-fields?": True
                            }
                        }
                    ]
                }
            }
        ]

        # Send request to delete the database
        response = self.send_data(delete_payload)

        # Check if response is valid before accessing status_code
        if response is None:
            logger.error(f"Failed to send delete request for '{entity_type}', response is None.")
            return False  # Or raise an exception

        if response.status_code == 200:
            logger.success(f'Database {database_name} in app {app_name} "{entity_type}" was successfully deleted.')
            return True
        else:
            logger.error(f'Error deleting entity type: {response.text}')
            return False

    def add_entity(self, app_name, database_name, list_data):
        # Initialize a list to hold all entity creation commands
        commands = []

        # Iterate over each data dictionary in the provided list
        for data in list_data:
            # Prepare the entity data``
            entity_data = {
                "command": "fibery.entity/create",
                "args": {
                    "type": f"{app_name}/{database_name}",
                    "entity": {
                        # Generate a UUID based on the "NameSurname" and "Age" fields (unique identifier)
                        "fibery/id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{data['NameSurname']}{data['Age']}"))
                    }
                }
            }

            # Add the fields from the data dictionary
            for key, value in data.items():
                entity_data["args"]["entity"][f"{database_name}/{key}"] = value

            # Append the entity creation command to the commands list
            commands.append(entity_data)

        # Send the batch request to create all entities
        response = self.send_data(commands)

        if response.status_code != 200:
            logger.error(f"Failed to the proccess addition data: {response.text}")
            return None

        logger.success(f"Data added to the database {database_name} in app {app_name}.")
        return response.json()

    def delete_entities(self, app_name, database_name, list_data):
        # Initialize a list to hold all entity deletion commands
        commands = []

        # Iterate over each data dictionary in the provided list
        for data in list_data:
            # Prepare the entity data
            entity_data = {
                "command": "fibery.entity/delete",
                "args": {
                    "type": f"{app_name}/{database_name}",
                    "entity": {
                        # Generate a UUID based on the "NameSurname" and "Age" fields (unique identifier)
                        "fibery/id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{data['NameSurname']}{data['Age']}"))
                    }
                }
            }

            # Append the entity deletion command to the commands list
            commands.append(entity_data)

        # Send the batch request to delete all entities
        response = self.send_data(commands)

        if response.status_code != 200:
            logger.error(f"Failed to delete data: {response.text}")
            return None

        logger.success(f"Data deleted successfully:")

        for index, result in  enumerate(response.json()):
            if result.get("success", False):
                logger.success(f"Delete data {list_data[index]} with result: {result['success']}")
            else:
                logger.error(f"Delete data {list_data[index]} with result: {result['success']}. Error: {result['result']['name']}")   
            # logger

        return response.json()
    
    def get_data(self, app_name, database_name, dict_fields):
        dict_fields = [key for key in dict_fields.keys()]
        data_fields = []
        for field in dict_fields:
            data_fields.append(f'{database_name}/{field}')

        data = [
                    {
                    "command": "fibery.entity/query",
                    "args": {
                        "query": {
                        "q/from": f"{app_name}/{database_name}",
                        "q/select": data_fields,
                        "q/limit": "q/no-limit"
                        }
                    }
                    }
                ]

        response = self.send_data(data)
        if response.status_code != 200:
            logger.error(f"Failed to get data: {response.text}")
            return None
        return response.json()  

def main(url, token, app_name, database_name, fields, list_data_entitys_add, list_data_entitys_delete):
    logger.debug(f"API_FIBERY_URL: {url}")

    fibery_agent = FiberyAgent(url, token)

    # Check connection
    logger.info(f"# Check connection in app {app_name}")
    schema = fibery_agent.get_schema()
    if schema.status_code != 200:
        logger.error(f"Failed to get schema: {schema.text}")
        exit(1)

    logger.success("Connection success")
    # logger.debug(schema.text)

    logger.info(f"# Start proccess create database {database_name} in app {app_name}")

    # Check data for create database
    if not url or not token:
        logger.error("API_FIBERY_URL and API_FIBERY_TOKEN environment variables are required.")
        exit(1)

    if fibery_agent.create_database(app_name = app_name, database_name = database_name, fields = fields):
        logger.debug(f"Database {database_name} exists in the application {app_name}")
        fields_data = fibery_agent.get_fields(app_name, database_name)
        logger.debug(f"List of field names and types in the database {database_name}: {fields_data}")

    time_countdown(5)

    logger.info(f"# Add data to database {database_name} in app {app_name}")
    data_write = fibery_agent.add_entity(app_name, database_name, list_data_entitys_add)
    logger.debug(data_write)

    time_countdown(5)

    logger.info(f"# Return data from database {database_name} in app {app_name}")

    data_read = fibery_agent.get_data(app_name, database_name, fields)
    logger.debug(data_read)

    # Create DataFrame, rename columns, return DataFrame
    df_data = pd.DataFrame(data_read[0]['result']) if data_read else data_read
    df_data.rename(columns={col: col.split('/')[-1] for col in df_data.columns}, inplace=True)
    
    logger.success(f"Data of the database {database_name}: \n{df_data}")

    time_countdown(5)

    logger.info(f"# Delete data to database {database_name} in app {app_name}")
    data_delete = fibery_agent.delete_entities(app_name, database_name, list_data_entitys_delete)
    logger.debug(data_delete)

    time_countdown(5)

    logger.info(f"# Delete the database {database_name} in app {app_name}")
    data_delete = fibery_agent.delete_database(app_name, database_name)
    logger.debug(f"Delete the database {database_name} in app {app_name}. Result {data_delete}.")

if __name__ == "__main__":
    url: str = os.getenv('API_FIBERY_URL')
    token: str = os.getenv('API_FIBERY_TOKEN')

    # Create database and fields
    app_name: str = 'TestSpace'
    database_name: str = 'Empoyees'
    fields: dict = {
                'NameSurname': 'text',
                'Age': 'int',
                'Manager': 'text',
                'Subdivision': 'text',
                'Salary': 'float', 
                'JoinDate': 'date-time',
                'IsActive': 'boolean',
            }

  
    list_data_entitys_add: list = [{
                'NameSurname': 'Stiven Fox',
                'Age': 25,
                'Manager': 'Tim Brown',
                'Subdivision': 'Department A',
                'Salary': 1000.10, 
                'JoinDate': '2023-01-01T00:00:00.000Z',
                'IsActive': True
            },
            {
                'NameSurname': 'Foxy Stivenson',
                'Age': 35,
                'Manager': 'Roger Smith',
                'Subdivision': 'Department B',
                'Salary': 2000.20, 
                'JoinDate': '2024-01-01T00:00:00.000Z',
                'IsActive': True
            }
            ]

    list_data_entitys_delete: list = [{
                'NameSurname': 'Stiven Fox',
                'Age': 25,
            },
            {
                'NameSurname': 'Foxy Stivenson',
                'Age': 35,
            }
            ]
    
    main(url, token, app_name, database_name, fields, list_data_entitys_add, list_data_entitys_delete)
