import boto3

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


table = dynamodb.Table("sammy-lagos-weather-table")

# https://dev.to/aws-builders/how-to-import-csv-data-into-dynamodb-using-lambda-and-s3-event-triggers-24io
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name'] 
    s3_file_name = event['Records'][0]['s3']['object']['key']
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    data = resp['Body'].read().decode("utf-8")
    weather_data = data.split("\n")[1:]
    
    for dataset in weather_data:
        if dataset.strip():  # Check if the line is not empty or contains only whitespace
            print(dataset)
            data = dataset.split(",")
            try:
                table.put_item(
                    Item={
                        "id": data[0],
                        "station_name": data[0],
                        "state": data[1],
                        'lga': data[2],
                        "weather_type": data[3],
                        "description": data[4],
                        "longitude": data[5],
                        "latitude": data[6],
                        "temp(F)": data[7],
                        "temp_min(F)": data[8],
                        "temp_max(F)": data[9],
                        "pressure(hPa)": data[10],
                        "humidity(%)": data[11],
                        "wind_speed(m/s)": data[12],
                        "wind_deg": data[13],
                        "wind_gust(m/s)": data[14],
                        "cloudiness(%)": data[15],
                        "sea_atmo_pressure(hPa)": data[16],
                        "grnd_atmo_pressure(hPa)": data[17],
                        "datetime":data[18]
                    }
                )
            except Exception as e:
                print(e)
