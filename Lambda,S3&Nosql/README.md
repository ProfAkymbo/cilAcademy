# Weather Data Collection and Storage Project
## Overview
This project demonstrates how to use AWS services, including Lambda, S3, EventBridge, and DynamoDB, to collect weather data from the Open Weather API for Lagos state in Nigeria. The data is collected every four hours using a Python script deployed as an AWS Lambda function, triggered by an EventBridge rule. The raw data is stored in an Amazon S3 data lake, and the cleaned data is stored in a DynamoDB database.

## Goal
The main goal of this project is to provide a practical example of how to leverage AWS services to collect, store, and process data efficiently. By following the steps in this tutorial, you will gain a better understanding of AWS Lambda, S3, EventBridge, and DynamoDB and how they can be integrated to create a data pipeline.

## Prerequisites
To complete this project, you will need:
1. An AWS account with the necessary permissions to create Lambda functions, S3 buckets, EventBridge rules, and DynamoDB tables.
2. An Open Weather API account and API key.
3. Python and the AWS SDK (boto3) installed on your local machine for testing and deployment.

## Reference
To get started with the project, you can follow this tutorial on Dev.to: [Link to Tutorial](https://dev.to/aws-builders/how-to-import-csv-data-into-dynamodb-using-lambda-and-s3-event-triggers-24io).

## Conclusion
Upon completing this project I have created a pipeline that collects weather data from the Open Weather API, stores raw and cleaned data in S3 data lakes, and stores the cleaned data in a DynamoDB database. This gives  valuable insights into AWS services and serverless architecture.