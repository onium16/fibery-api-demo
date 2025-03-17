import unittest
from unittest.mock import patch, MagicMock, ANY  # ✅ Импортировали ANY
import requests  # ✅ Импортировали requests
from main import FiberyAgent, FiberyError
from logger_custom import LoggerCustom
from main import main as fibery_agent_main 

# Set the logger
log_file_path = 'log/_tests-main.log'
LEVEL_LOGGER = "DEBUG"
logger = LoggerCustom(log_file_path, LEVEL_LOGGER).get_logger()

class TestFiberyAgent(unittest.TestCase):
    def setUp(self):
        self.agent = FiberyAgent('https://api.fibery.io', 'your_token')
    @patch('requests.post')
    def test_get_schema(self, mock_post):
        """Test get_schema when Fibery API returns a valid response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"success": true}'
        mock_post.return_value = mock_response

        response = self.agent.get_schema()

        self.assertEqual(response.status_code, 200)
        mock_post.assert_called_once_with(
            self.agent.url, headers=ANY, json=[{"command": "fibery.schema/query"}]
        )

    @patch('requests.post')
    def test_get_schema_error(self, mock_post):
        """Test get_schema when Fibery API returns an error response."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = '{"error": "Bad Request"}'
        mock_post.return_value = mock_response

        with self.assertRaises(FiberyError) as context:
            self.agent.get_schema()
        
        self.assertIn("[Fibery Error] Failed to get schema", str(context.exception))
        mock_post.assert_called_once_with(
            self.agent.url, headers=ANY, json=[{"command": "fibery.schema/query"}]
        )

    @patch('requests.post')
    def test_get_schema_no_response(self, mock_post):
        """Test get_schema when Fibery API request fails completely."""
        mock_post.side_effect = requests.RequestException("Mocked request exception")

        with self.assertRaises(FiberyError) as context:
            self.agent.get_schema()

        self.assertIn("Failed to send data to Fibery", str(context.exception))
        mock_post.assert_called_once_with(
            self.agent.url, headers=ANY, json=[{"command": "fibery.schema/query"}]
        )

    @patch('requests.post')
    def test_add_entity(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}  
        mock_post.return_value = mock_response

        entity = {'NameSurname': 'Test Entity', 'Age': 30}
        response = self.agent.add_entity('Test App', 'Test Database', [entity])
        self.assertEqual(response["success"], True)  # Ожидаем корректный формат словаря

    @patch('requests.post')
    def test_delete_entities(self, mock_post):
        """Тест удаления сущностей"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"success": True}]  # Должен быть список словарей
        mock_post.return_value = mock_response

        entities = [{'NameSurname': 'Test Entity', 'Age': 30}]  
        response = self.agent.delete_entities('Test App', 'Test Database', entities)

        logger.debug(f"Тестовый ответ API: {response}")  # Отладочный вывод
        self.assertIsInstance(response, list)  # Проверяем, что ответ - список
        self.assertEqual(response[0].get("success"), True)  # Проверяем наличие success

    @patch('requests.post')
    def test_get_data(self, mock_post):
        """Тест получения данных"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"result": [{"NameSurname": "Test"}]}]  # Должен быть список
        mock_post.return_value = mock_response

        fields = {'NameSurname': 'text'}  
        response = self.agent.get_data('Test App', 'Test Database', fields)

        logger.debug(f"Ответ теста: {response}")  # Отладочный вывод
        self.assertIsInstance(response, list)  # Проверяем, что это список
        self.assertEqual(response[0]["result"][0]["NameSurname"], "Test")  # Проверяем, что имя соответствует ожиданиям


    @patch('requests.post')
    def test_delete_entities_with_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "No data provided for entity deletion."}
        mock_post.return_value = mock_response

        with self.assertRaises(FiberyError) as context:
            self.agent.delete_entities('Test App', 'Test Database', [])

        self.assertIn("No data provided for entity deletion.", str(context.exception))

    @patch('requests.post')
    def test_get_data_with_error(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "No fields provided for data retrieval."}
        mock_post.return_value = mock_response

        with self.assertRaises(FiberyError) as context:
            self.agent.get_data('Test App', 'Test Database', {})

        self.assertIn("No fields provided for data retrieval.", str(context.exception))
            
    @patch.object(FiberyAgent, 'get_schema', side_effect=FiberyError("Mocked FiberyError"))
    @patch.object(FiberyAgent, 'add_entity', return_value=MagicMock(status_code=200))
    @patch.object(FiberyAgent, 'delete_entities', return_value=MagicMock(status_code=200))
    @patch.object(FiberyAgent, 'get_data', return_value=MagicMock(status_code=200))
    @patch.object(FiberyAgent, 'create_database', return_value=False)
    @patch.object(FiberyAgent, 'delete_database', return_value=False)
    def test_main(self, mock_delete_database, mock_create_database, mock_get_schema, 
                    mock_get_data, mock_delete_entities, mock_add_entity):
        with patch.object(FiberyAgent, 'get_schema', side_effect=FiberyError("Mocked FiberyError")):
            with self.assertRaises(SystemExit) as context:
                fibery_agent_main(
                    'https://api.fibery.io', 'your_token', 'Test App', 'Test Database',
                    {'NameSurname': 'text'}, 
                    [{'NameSurname': 'Test Entity', 'Age': 30}], 
                    [{'NameSurname': 'Test Entity', 'Age': 30}]
                )

        self.assertEqual(context.exception.code, 1)  # Ожидаем sys.exit(1)

        # Проверяем, что get_schema вызван
        mock_get_data.assert_not_called() 

        # Убеждаемся, что get_data НЕ вызывается
        mock_get_data.assert_not_called()

        # Остальные моки НЕ вызываются
        mock_create_database.assert_not_called()
        mock_add_entity.assert_not_called()
        mock_delete_entities.assert_not_called()
        mock_delete_database.assert_not_called()



if __name__ == '__main__':
    unittest.main()
