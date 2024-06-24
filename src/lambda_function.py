import os
import boto3
import requests
import csv
import json

s3_client = boto3.client('s3')
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

def read_csv_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8').splitlines()
    return list(csv.DictReader(content, delimiter=';'))

def calculate_accuracy(predicted_qualities, actual_qualities):
    total = len(predicted_qualities)
    correct = sum(1 for p, a in zip(predicted_qualities, actual_qualities) if p == a)
    accuracy = (correct / total) * 100
    return accuracy

def lambda_handler(event, context):
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    cognito_client_id = os.environ['COGNITO_CLIENT_ID']
    cognito_client_secret = os.environ['COGNITO_CLIENT_SECRET']
    cognito_token_url = os.environ['COGNITO_TOKEN_URL']
    api_url = os.environ['API_URL']
    cognito_scope = os.environ['COGNITO_SCOPE']
    bucket_name = os.environ['BUCKET_NAME']
    csv_key = os.environ['CSV_KEY']

    try:
        # Get token from Cognito
        token = get_cognito_token(cognito_client_id, cognito_client_secret, cognito_token_url, cognito_scope)
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        # Read and process CSV data from S3
        csv_data = read_csv_from_s3(bucket_name, csv_key)
        
        total_data = len(csv_data)
        predicted_qualities = []
        actual_qualities = [float(row['quality']) for row in csv_data]

        # Convert keys to match what the API expects
        def convert_keys(row):
            return {
                "fixed_acidity": float(row['fixed acidity']),
                "volatile_acidity": float(row['volatile acidity']),
                "citric_acid": float(row['citric acid']),
                "residual_sugar": float(row['residual sugar']),
                "chlorides": float(row['chlorides']),
                "free_sulfur_dioxide": float(row['free sulfur dioxide']),
                "total_sulfur_dioxide": float(row['total sulfur dioxide']),
                "density": float(row['density']),
                "pH": float(row['pH']),
                "sulphates": float(row['sulphates']),
                "alcohol": float(row['alcohol'])
            }

        # Process data in batches of 12
        batch_size = 12
        for i in range(0, len(csv_data), batch_size):
            batch = csv_data[i:i+batch_size]
            batch = [convert_keys(row) for row in batch]
            # Log batch being sent
            print(f"Sending batch: {batch}")
            # Send batch to API for prediction
            response = requests.post(api_url, headers=headers, json=batch)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as err:
                print(f"Request failed: {err}")
                print(f"Response text: {response.text}")
                raise
            
            # Log response
            print(f"Response received: {response.json()}")
            
            # Append predicted qualities to list
            predicted_qualities.extend(response.json().get('qualities'))
        
        # Calculate accuracy
        accuracy = calculate_accuracy(predicted_qualities, actual_qualities)

        # Create message for SNS
        message = f"Total data points: {total_data}\n"
        message += f"Accuracy: {accuracy}%\n"
        if accuracy < 80:
            message += "Model may be outdated."
        else:
            message += "Model is up to date."

        # Send message to SNS
        sns_response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject='Model Accuracy Report'
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'qualities': predicted_qualities})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
