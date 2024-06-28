import unittest
from unittest.mock import patch, Mock
import os
import json
from lambda_function import lambda_handler, get_cognito_token, read_csv_from_s3, calculate_accuracy, get_clients

class TestLambdaFunction(unittest.TestCase):

    def setUp(self):
        os.environ['SNS_TOPIC_ARN'] = 'test_sns_topic_arn'
        os.environ['COGNITO_CLIENT_ID'] = 'test_client_id'
        os.environ['COGNITO_CLIENT_SECRET'] = 'test_client_secret'
        os.environ['COGNITO_TOKEN_URL'] = 'https://test_cognito_token_url'
        os.environ['API_URL'] = 'https://test_api_url'
        os.environ['COGNITO_SCOPE'] = 'test_scope'
        os.environ['BUCKET_NAME'] = 'test_bucket'
        os.environ['CSV_KEY'] = 'test_csv_key.csv'
        os.environ['AWS_DEFAULT_REGION'] = 'eu-west-1'

    @patch('lambda_function.requests.post')
    def test_get_cognito_token(self, mock_post):
        mock_response = Mock()
        mock_response.json.return_value = {'access_token': 'test_token'}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        token = get_cognito_token('test_client_id', 'test_client_secret', 'https://test_cognito_token_url', 'test_scope')
        self.assertEqual(token, 'test_token')

    @patch('lambda_function.boto3.client')
    def test_read_csv_from_s3(self, mock_boto_client):
        mock_s3_client = Mock()
        mock_response = Mock()
        body_mock = Mock()
        body_mock.read.return_value = 'fixed acidity;volatile acidity;citric acid;residual sugar;chlorides;free sulfur dioxide;total sulfur dioxide;density;pH;sulphates;alcohol;quality\n7.4;0.7;0;1.9;0.076;11;34;0.9978;3.51;0.56;9.4;5\n'.encode('utf-8')
        mock_response.body = body_mock
        mock_s3_client.get_object.return_value = {'Body': body_mock}
        mock_boto_client.return_value = mock_s3_client

        s3_client, _ = get_clients()
        csv_data = read_csv_from_s3(s3_client, 'test_bucket', 'test_csv_key.csv')
        expected_data = [{'fixed acidity': '7.4', 'volatile acidity': '0.7', 'citric acid': '0', 'residual sugar': '1.9', 'chlorides': '0.076', 'free sulfur dioxide': '11', 'total sulfur dioxide': '34', 'density': '0.9978', 'pH': '3.51', 'sulphates': '0.56', 'alcohol': '9.4', 'quality': '5'}]
        self.assertEqual(csv_data, expected_data)

    def test_calculate_accuracy(self):
        predicted_qualities = [5.0, 6.0, 5.0]
        actual_qualities = [5.0, 6.0, 7.0]
        accuracy = calculate_accuracy(predicted_qualities, actual_qualities)
        self.assertEqual(accuracy, 66.66666666666666)

    @patch('lambda_function.requests.post')
    @patch('lambda_function.boto3.client')
    @patch('lambda_function.read_csv_from_s3')
    @patch('lambda_function.get_cognito_token')
    def test_lambda_handler(self, mock_get_cognito_token, mock_read_csv_from_s3, mock_boto_client, mock_post):
        mock_get_cognito_token.return_value = 'test_token'
        mock_read_csv_from_s3.return_value = [{'fixed acidity': '7.4', 'volatile acidity': '0.7', 'citric acid': '0', 'residual sugar': '1.9', 'chlorides': '0.076', 'free sulfur dioxide': '11', 'total sulfur dioxide': '34', 'density': '0.9978', 'pH': '3.51', 'sulphates': '0.56', 'alcohol': '9.4', 'quality': '5'}]
        mock_response = Mock()
        mock_response.json.return_value = {'qualities': [5.0]}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        mock_s3_client = Mock()
        mock_sns_client = Mock()
        mock_boto_client.side_effect = lambda service_name, *args, **kwargs: mock_s3_client if service_name == 's3' else mock_sns_client

        event = {}
        context = {}
        response = lambda_handler(event, context, s3_client=mock_s3_client, sns_client=mock_sns_client)

        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])
        self.assertIn('qualities', body)
        self.assertEqual(body['qualities'], [5.0])
        mock_sns_client.publish.assert_called_once()

if __name__ == '__main__':
    unittest.main()
