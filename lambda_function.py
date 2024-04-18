import boto3
import csv
import json
import time
from datetime import datetime
from io import StringIO

# Initialize the S3 client
s3 = boto3.client('s3')

# Initialize the TimestreamWrite client
timestream_write = boto3.client('timestream-write')

# S3 bucket names
bucket_name = 'bcpbucket1'
s3_drain_bucket_name = 'sdrainbucket2'

# Timestream database and table ARNs
database_arn = 'lambda_injected_db'
table_arn = 'lambda_injected_table'

# Function to read last record from S3 bucket
def read_last_readings(device_id):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=f'{device_id}_lastReadings.json')
        data = response['Body'].read().decode('utf-8')
        return json.loads(data)
    except Exception as e:
        print("Error reading last readings from S3:", e)
        return {'lastX': None, 'lastY': None, 'lastTime': None}

# Function to write last device id to S3 bucket
def write_last_readings(lastX, lastY, device_id):
    try:
        current_time = int(time.time())
        s3.put_object(Bucket=bucket_name, Key=f'{device_id}_lastReadings.json', Body=json.dumps({'lastX': lastX, 'lastY': lastY, 'lastTime': current_time}), ContentType='application/json')
    except Exception as e:
        print("Error writing last readings to S3:", e)

# Function to write sensor reading into timestream db
def write_to_timestream(x_axis, y_axis, total_velocity, device_id, battery, longitude, latitude, timestamp):
    try:
        record = {
            'Time': str(timestamp),
            'Dimensions': [{'Name': 'device_id', 'Value': device_id}],
            'MeasureName': 'x_axis,y_axis,total_velocity,battery,longitude,latitude',
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                {'Name': 'x_axis', 'Value': str(x_axis), 'Type': 'DOUBLE'},
                {'Name': 'y_axis', 'Value': str(y_axis), 'Type': 'DOUBLE'},
                {'Name': 'total_velocity', 'Value': str(total_velocity), 'Type': 'DOUBLE'},
                {'Name': 'battery', 'Value': str(battery), 'Type': 'DOUBLE'},
                {'Name': 'longitude', 'Value': str(longitude), 'Type': 'DOUBLE'},
                {'Name': 'latitude', 'Value': str(latitude), 'Type': 'DOUBLE'}
            ],
            'TimeUnit': 'MICROSECONDS'
        }
        timestream_write.write_records(DatabaseName=database_arn, TableName=table_arn, Records=[record])
    except Exception as e:
        print("Error writing data to Timestream:", e)

def update_csv_flowrate(device_id, total_velocity):
    try:
        # Read CSV file from S3
        response = s3.get_object(Bucket=s3_drain_bucket_name, Key='coordinates.csv')
        content = response['Body'].read().decode('utf-8')
        
        # Update 'flowrate' column based on 'device_id'
        updated_content = []
        csv_reader = csv.reader(StringIO(content))
        for row in csv_reader:
            if row and row[0] == device_id:
                row[3] = str(total_velocity)  # Assuming 'flowrate' column is at index 3
            updated_content.append(row)
        
        # Write updated CSV file back to S3
        updated_csv = StringIO()
        csv_writer = csv.writer(updated_csv)
        csv_writer.writerows(updated_content)
        s3.put_object(Bucket=s3_drain_bucket_name, Key='coordinates.csv', Body=updated_csv.getvalue())
    except Exception as e:
        print("Error updating CSV file:", e)

def lambda_handler(event, context):
    if 'x_axis' in event and 'y_axis' in event and 'device_id' in event and 'battery' in event and 'longitude' in event and 'latitude' in event:
        device_id = event['device_id']
        battery = event['battery']
        longitude = event['longitude']
        latitude = event['latitude']
        
        last_readings = read_last_readings(device_id)
        lastX = last_readings['lastX']
        lastY = last_readings['lastY']
        lastTime = last_readings['lastTime']

        if lastX is not None and lastY is not None and lastTime is not None:
            delta_x = event['x_axis'] - lastX
            delta_y = event['y_axis'] - lastY
            current_time = int(time.time())
            time_delta = current_time - lastTime
            total_velocity = ((delta_x ** 2 + delta_y ** 2) ** 0.5) / time_delta
            current_time_micros = int(time.time() * 1000000)
            write_last_readings(event['x_axis'], event['y_axis'], device_id)
            write_to_timestream(event['x_axis'], event['y_axis'], total_velocity, device_id, battery, longitude, latitude, current_time_micros)
            
            # Update flowrate in CSV
            update_csv_flowrate(device_id, total_velocity)
            
            return {
                'x_axis': event['x_axis'],
                'y_axis': event['y_axis'],
                'totalVelocity': total_velocity,
                'device_id': device_id,
                'battery': battery,
                'longitude': longitude,
                'latitude': latitude,
                'time': datetime.utcfromtimestamp(current_time_micros // 1000000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
        else:
            write_last_readings(event['x_axis'], event['y_axis'], device_id)
            update_csv_flowrate(device_id, total_velocity)
            return {
                'x_axis': event['x_axis'],
                'y_axis': event['y_axis'],
                'totalVelocity': None,
                'device_id': device_id,
                'battery': battery,
                'longitude': longitude,
                'latitude': latitude,
                'time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
    else:
        raise ValueError("Invalid data format. 'x_axis', 'y_axis', 'device_id', 'longitude', 'latitude', and 'battery' properties are required.")
