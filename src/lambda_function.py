import os
import boto3
import requests

sns_client = boto3.client('sns')

def get_cognito_token(client_id, client_secret, token_url, scope):
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(token_url, data=payload, headers=headers)
    response.raise_for_status()
    return response.json()['access_token']

def lambda_handler(event, context):
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    cognito_client_id = os.environ['COGNITO_CLIENT_ID']
    cognito_client_secret = os.environ['COGNITO_CLIENT_SECRET']
    cognito_token_url = os.environ['COGNITO_TOKEN_URL']
    api_url = os.environ['API_URL']
    cognito_scope = os.environ['COGNITO_SCOPE']

    try:
        # Get token from Cognito
        token = get_cognito_token(cognito_client_id, cognito_client_secret, cognito_token_url, cognito_scope)
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        data = {
            "fixed_acidity": 6,
            "volatile_acidity": 0.31,
            "citric_acid": 0.47,
            "residual_sugar": 3.6,
            "chlorides": 0.067,
            "free_sulfur_dioxide": 18,
            "total_sulfur_dioxide": 42,
            "density": 0.99549,
            "pH": 3.39,
            "sulphates": 0.66,
            "alcohol": 11,
            "quality": 6.0  # Esta es la calidad real
        }

        response = requests.post(api_url, headers=headers, json=data)
        response.raise_for_status()
        predicted_quality = response.json().get('quality')

        # Obtener la calidad real del evento
        actual_quality = data.get('quality')
        accuracy = calculate_accuracy(predicted_quality, actual_quality)

        message = f"Predicted quality: {predicted_quality}, Actual quality: {actual_quality}, Accuracy: {accuracy}%\n"
        if accuracy < 80:
            message += "Model may be outdated."
        else:
            message += "Model is up to date."

        sns_response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject='Model Accuracy Report'
        )

        return {
            'statusCode': 200,
            'body': 'Email sent with accuracy report!'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }

def calculate_accuracy(predicted, actual):
    if predicted == actual:
        return 100
    else:
        return (1 - abs(predicted - actual) / actual) * 100
