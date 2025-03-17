import os
import sys
import time
import pandas as pd
import uuid
import requests
from logger_custom import LoggerCustom
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Union

from constants import SUPPORTED_FIELD_TYPES, FIBERY_FIELD_GENERAL

# Set the logger
log_file_path = 'log/main.log'
LEVEL_LOGGER = "INFO"
logger = LoggerCustom(log_file_path, LEVEL_LOGGER).get_logger()

# Добавляем обработчик для вывода в консоль
load_dotenv()


class FiberyError(Exception):
    """Custom error class for Fibery API."""
    pass

def time_countdown(seconds: int) -> None:
    """Counts down from the given number of seconds, displaying the time left."""
    while seconds > 0:
        sys.stdout.write(f'\rThere are {seconds} seconds left until the next stage')
        sys.stdout.flush()
        time.sleep(1)
        seconds -= 1
    logger.info('Time has passed!          ')                                                                 

class FiberyAgent:
    def __init__(self, url: str, token: str) -> None:
        """
        Initialize FiberyAgent with API URL and token.

        Args:
            url (str): The URL of the Fibery API.
            token (str): The authentication token for the Fibery API.
        """
        self.url = url
        self.token = token

    def send_data(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Optional[requests.Response]:
        """
        Send a request to the Fibery API.

        Args:
            data (Union[Dict[str, Any], List[Dict[str, Any]]]): The data to be sent in the request.

        Returns:
            Optional(requests.Response)/None: The response object containing the schema data, or None if an error occurs.
        """
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(self.url, headers=headers, json=data)
            if response is None:
                logger.error("Received None response from Fibery API")
                raise FiberyError("Failed to send data to Fibery: No response")
            
            response.raise_for_status()  # Raise exception for HTTP errors
            return response
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            raise FiberyError(f"Failed to send data to Fibery: {e}")
       
    def get_schema(self) -> Optional[requests.Response]:
        """
        Retrieve the schema from Fibery.
        
        Returns:
            Response: The response object containing the schema data, or None if an error occurs.
        """
        data = [{"command": "fibery.schema/query"}]
        data_schema = self.send_data(data)

        if data_schema.status_code != 200:
            error_msg = f"[Fibery Error] Failed to get schema: {data_schema.text if data_schema else 'No response'}"
            logger.error(error_msg)
            raise FiberyError(error_msg)

        return data_schema
    
    def create_database(self, app_name: str, database_name: str, fields: Dict[str, str]) -> bool:
            """
            Create a database in Fibery.
            
            Args:
                app_name (str): The name of the Fibery app.
                database_name (str): The name of the database.
                fields (Dict[str, str]): A dictionary with field names as keys and field types as values.

            Returns:
                bool: True if the database was successfully created, False otherwise.

            """
            
            # List to store information about fields
            fibery_fields: List[Dict[str, Union[str, Dict[str, bool]]]] = []

            # Processing each custom field
            for count, (field_name, field_type) in enumerate(fields.items()):
                # Checking support for the specified field type
                if field_type not in SUPPORTED_FIELD_TYPES:
                    raise ValueError(f"Field type '{field_type}' is not supported. "
                                    f"Allowed types: {', '.join(SUPPORTED_FIELD_TYPES.keys())}.")

                # If this is the first field, set it as the primary field
                fibery_field = {
                    "fibery/name": f"{database_name}/{field_name}",
                    "fibery/type": "fibery/text" if count == 0 else SUPPORTED_FIELD_TYPES[field_type],
                    "fibery/meta": {
                        "fibery/secured?": False,
                        "ui/title?": count == 0
                    }
                }

                fibery_fields.append(fibery_field)

                # Adding system fields if it's the first field
                if count == 0:
                    fibery_fields.extend(FIBERY_FIELD_GENERAL)

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

            response = self.send_data(general_data)

            if not response or response.status_code != 200:
                logger.error(f"[Fibery Error] Failed to create database: {response.text if response else 'No response'}")
                return False

            # Parse the JSON content
            response_json = response.json()
            
            if isinstance(response_json, list) and response_json:
                success = response_json[0].get("success", None)
                if not success:
                    error_message = response_json[0].get("result", {}).get("message", "Unknown error")
                    if 'database already exists' in error_message:
                        logger.warning(f"Database '{database_name}' already exists: {error_message}")
                        return True
                    else:
                        logger.error(f"Database '{database_name}' could not be created in app '{app_name}'. "
                                    f"Ensure the app '{app_name}' exists. Error: {error_message}")
                        return False
                else:
                    logger.success(f"Database '{database_name}' was created successfully in app '{app_name}'.")
                    return True

            logger.error("Unexpected response format or empty response.")
            return False
    
    def get_fields(self, app_name: str, database_name: str) -> Dict[str, str]:
        """
        Retrieve field names and types for a given database in Fibery.

        Args:
            app_name (str): The name of the Fibery app.
            database_name (str): The name of the database.

        Returns:
            Dict: A dictionary with field names as keys and field types as values.
        """
        data = [{"command": "fibery.schema/query", "database": f"{app_name}/{database_name}"}]
        response = self.send_data(data)

        if not response or response.status_code != 200:
            logger.error(f"Failed to get fields: {response.text if response else 'No response'}")
            return {}

        try:
            response_json = response.json()
            entity_types = response_json[0].get("result", {}).get("fibery/types", [])

            data_fields = {}
            for entity_type in entity_types:
                fibery_name = entity_type.get('fibery/name', '')
                if fibery_name == f"{app_name}/{database_name}":
                    raw_fields = entity_type.get('fibery/fields', [])
                    filtered_fields = [
                        field for field in raw_fields
                        if field.get('fibery/name', '').startswith(f"{app_name}/") or
                        field.get('fibery/name', '').startswith(f"{database_name}/")
                    ]

                    for field in filtered_fields:
                        field_name = field.get('fibery/name', '').split('/')[-1].strip()
                        field_type = field.get('fibery/type', '').split('/')[-1].strip()
                        if field_name and field_type:
                            data_fields[field_name] = field_type

            return data_fields

        except Exception as e:
            logger.error(f"Error processing Fibery response: {e}")
            return {}

    def delete_database(self, app_name: str, database_name: str) -> bool:
        """
        Delete a database in Fibery.

        Args:
            app_name (str): The name of the Fibery app.
            database_name (str): The name of the database.

        Returns:
            bool: True if the database was successfully deleted, False otherwise.
        """
        entity_type = f"{app_name}/{database_name}"

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

        try:
            response: Optional[requests.Response] = self.send_data(delete_payload)

            if response is None:
                logger.error(f"Failed to send delete request for '{entity_type}', response is None.")
                return False

            if response.status_code == 200:
                logger.success(f"Database '{database_name}' in app '{app_name}' was successfully deleted.")
                return True
            else:
                logger.error(f"Error deleting database '{database_name}' in app '{app_name}': {response.text}")
                return False

        except Exception as e:
            logger.error(f"Exception while deleting database '{database_name}': {e}")
            return False

    def add_entity(self, app_name: str, database_name: str, list_data: List[Dict[str, any]]) -> Optional[dict]:
        """Add multiple entities to a Fibery database.

        Args:
            app_name (str): The name of the Fibery app.
            database_name (str): The name of the Fibery database.
            list_data (List[Dict[str, any]]): A list of dictionaries containing entity data.

        Returns:
            Optional[dict]: Response JSON if successful, otherwise None.
        """
        if not list_data:
            error_msg = f"No data provided for entity creation."
            logger.error(error_msg)
            raise FiberyError(error_msg)

        commands = []

        for data in list_data:
            try:
                unique_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{data['NameSurname']}{data['Age']}"))
            except KeyError as e:
                logger.error(f"Missing required key: {e}. Skipping entity creation.")
                continue  # Skip this entry if essential fields are missing

            entity_data = {
                "command": "fibery.entity/create",
                "args": {
                    "type": f"{app_name}/{database_name}",
                    "entity": {
                        "fibery/id": unique_id
                    }
                }
            }

            for key, value in data.items():
                entity_data["args"]["entity"][f"{database_name}/{key}"] = value

            commands.append(entity_data)

        if not commands:
            error_msg = f"No valid entities to add."
            logger.warning(error_msg)
            raise FiberyError(error_msg)

        try:
            response = self.send_data(commands)

            if response is None or response.status_code != 200:
                error_msg = f"Failed to process entity addition: {response.text if response else 'No response received'}"
                logger.error(error_msg)
                raise FiberyError(error_msg)

            logger.success(f"Data successfully added to the database '{database_name}' in app '{app_name}'.")
            return response.json()

        except Exception as e:
            error_msg = f"Unexpected error while adding entities: {e}"
            logger.error(error_msg)
            raise FiberyError(error_msg)

    def delete_entities(self, app_name: str, database_name: str, list_data: List[Dict[str, any]]) -> Optional[dict]:
        """
        Delete multiple entities from a Fibery database.

        Args:
            app_name (str): The name of the Fibery app.
            database_name (str): The name of the Fibery database.
            list_data (List[Dict[str, any]]): A list of dictionaries containing entity identifiers.

        Returns:
            Optional[dict]: Response JSON if successful, otherwise None.
        """
        if not list_data:
            error_msg = f"No data provided for entity deletion."
            logger.warning(error_msg)
            raise FiberyError(error_msg)

        commands = []

        for data in list_data:
            try:
                unique_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{data['NameSurname']}{data['Age']}"))
            except KeyError as e:
                logger.error(f"Missing required key: {e}. Skipping entity deletion.")
                continue  # Skip this entry if essential fields are missing

            entity_data = {
                "command": "fibery.entity/delete",
                "args": {
                    "type": f"{app_name}/{database_name}",
                    "entity": {
                        "fibery/id": unique_id
                    }
                }
            }

            commands.append(entity_data)

        if not commands:
            error_msg = f"No valid entities to delete."
            logger.warning(error_msg)
            raise FiberyError(error_msg)

        try:
            response = self.send_data(commands)

            if response is None or response.status_code != 200:
                error_msg = f"Failed to delete entities: {response.text if response else 'No response received'}"
                logger.warning(error_msg)
                raise FiberyError(error_msg)

            response_json = response.json()
            logger.success("Entities processed for deletion.")

            for index, result in enumerate(response_json):
                entity_info = list_data[index]
                if result.get("success", False):
                    logger.success(f"Successfully deleted: {entity_info}")
                else:
                    error_message = result.get("result", {}).get("name", "Unknown error")
                    logger.error(f"Failed to delete: {entity_info}. Error: {error_message}")

            return response_json

        except Exception as e:
            logger.error(f"Unexpected error during entity deletion: {e}")
            raise

    def get_data(self, app_name: str, database_name: str, dict_fields: Dict[str, str]) -> Optional[List[Dict[str, any]]]:
        """
        Retrieve data from a Fibery database.

        Args:
            app_name (str): The name of the Fibery app.
            database_name (str): The name of the Fibery database.
            dict_fields (Dict[str, str]): A dictionary of field names to retrieve.

        Returns:
            Optional[List[Dict[str, any]]]: Retrieved data as a list of dictionaries or None if an error occurs.
        """
        if not dict_fields:
            error_msg = f"No fields provided for data retrieval."
            logger.warning(error_msg)
            raise FiberyError(error_msg)

        data_fields = [f"{database_name}/{field}" for field in dict_fields.keys()]

        query_payload = [
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

        try:
            response = self.send_data(query_payload)

            if response is None or response.status_code != 200:
                error_msg = f"Failed to retrieve data: {response.text if response else 'No response received'}"
                logger.error(error_msg)
                raise FiberyError(error_msg)

            response_json = response.json()

            if not isinstance(response_json, list) or not response_json:
                error_msg = f"Received empty or invalid response format."
                logger.warning(error_msg)
                raise FiberyError(error_msg)
        
            logger.success(f"Successfully retrieved {len(response_json[0].get('result', []))} records from {database_name}.")
            return response_json
            

        except Exception as e:
            logger.error(f"Unexpected error during data retrieval: {e}")
            raise  

def main(
    url: str, 
    token: str, 
    app_name: str, 
    database_name: str, 
    fields: Dict[str, str], 
    list_data_entities_add: List[Dict[str, Any]], 
    list_data_entities_delete: List[Dict[str, Any]]
):
    """
    Main process for interacting with the Fibery API.

    Args:
        url (str): Fibery API URL.
        token (str): Authentication token for the API.
        app_name (str): Name of the Fibery app.
        database_name (str): Name of the database.
        fields (Dict[str, str]): Dictionary of database fields (name -> type).
        list_data_entities_add (List[Dict[str, Any]]): List of entities to add.
        list_data_entities_delete (List[Dict[str, Any]]): List of entities to delete.
    """
    try:
        logger.debug(f"API_FIBERY_URL: {url}")

        fibery_agent = FiberyAgent(url, token)

        # Check connection with the API
        logger.info(f"# Checking connection to the app {app_name}")
        schema = fibery_agent.get_schema()
        
        if schema is None or schema.status_code != 200:
            logger.error(f"Failed to retrieve schema: {schema.text if schema else 'No response'}")
            sys.exit(1)

        logger.success("Connection established.")

        # Validate input data
        if not url or not token:
            logger.error("API_FIBERY_URL and API_FIBERY_TOKEN environment variables are required.")
            sys.exit(1)

        logger.info(f"# Creating database {database_name} in app {app_name}")
        
        if fibery_agent.create_database(app_name=app_name, database_name=database_name, fields=fields):
            logger.debug(f"Database {database_name} exists in app {app_name}")
            fields_data = fibery_agent.get_fields(app_name, database_name)
            logger.debug(f"List of fields in database {database_name}: {fields_data}")

        time_countdown(5)

        # Adding data
        logger.info(f"# Adding data to {database_name}")
        data_write = fibery_agent.add_entity(app_name, database_name, list_data_entities_add)
        logger.debug(data_write)

        time_countdown(5)

        # Retrieving data
        logger.info(f"# Retrieving data from {database_name}")
        data_read_json = fibery_agent.get_data(app_name, database_name, fields)
        data_read = data_read_json[0].get("result", [])
        logger.debug(data_read)

        if not data_read:
            logger.warning("No data was retrieved.")
        else:
            df_data = pd.DataFrame(data_read)
            df_data.rename(columns={col: col.split('/')[-1] for col in df_data.columns}, inplace=True)
            logger.success(f"Retrieved data from {database_name}: \n{df_data}")

        time_countdown(5)

        # Deleting data
        logger.info(f"# Deleting data from {database_name}")
        data_delete = fibery_agent.delete_entities(app_name, database_name, list_data_entities_delete)
        logger.debug(data_delete)

        time_countdown(5)

        # Deleting the database
        logger.info(f"# Deleting database {database_name}")
        delete_result = fibery_agent.delete_database(app_name, database_name)
        logger.debug(f"Database {database_name} deleted. Result: {delete_result}")

    except FiberyError as e:
        logger.error(f"Failed to retrieve schema: {e}")
        sys.exit(1) 

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

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
