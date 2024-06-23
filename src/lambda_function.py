import os
import boto3

sns_client = boto3.client('sns')

def lambda_handler(event, context):
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    
    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message='Hello, world!',
            Subject='Test Email'
        )
        return {
            'statusCode': 200,
            'body': 'Email sent!'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
}
