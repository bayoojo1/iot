import boto3
import json
import time
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')

# Initialize the TimestreamWrite client
timestream_write = boto3.client('timestream-write')

# S3 bucket name for storing the last readings
bucket_name = 'bcpbucket1'

# Timestream database and table ARNs
database_arn = 'lambda_injected_db'
table_arn = 'lambda_injected_table'

# Function to read the last readings from S3
def read_last_readings(device_id):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=f'{device_id}_lastReadings.json')
        data = response['Body'].read().decode('utf-8')
        return json.loads(data)
    except Exception as e:
        print("Error reading last readings from S3:", e)
        return {'lastX': None, 'lastY': None, 'lastTime': None}

# Function to write the last readings to S3
def write_last_readings(lastX, lastY, device_id):
    try:
        current_time = int(time.time())
        s3.put_object(Bucket=bucket_name, Key=f'{device_id}_lastReadings.json', Body=json.dumps({'lastX': lastX, 'lastY': lastY, 'lastTime': current_time}), ContentType='application/json')
    except Exception as e:
        print("Error writing last readings to S3:", e)

# Function to write data to Amazon Timestream
def write_to_timestream(x_axis, y_axis, total_velocity, device_id, battery, timestamp):
    try:
        # Format the record
        record = {
            'Time': str(timestamp),  # Use the provided timestamp directly
            'Dimensions': [{'Name': 'device_id', 'Value': device_id}],
            'MeasureName': 'x_axis,y_axis,total_velocity,battery',  # Combine all measure names into a single string
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                {
                    'Name': 'x_axis',
                    'Value': str(x_axis),
                    'Type': 'DOUBLE'
                },
                {
                    'Name': 'y_axis',
                    'Value': str(y_axis),
                    'Type': 'DOUBLE'
                },
                {
                    'Name': 'total_velocity',
                    'Value': str(total_velocity),
                    'Type': 'DOUBLE'
                },
                {
                    'Name': 'battery',
                    'Value': str(battery),
                    'Type': 'DOUBLE'
                }
            ],
            'TimeUnit': 'MICROSECONDS'
        }

        # Write the record to Timestream
        timestream_write.write_records(DatabaseName=database_arn, TableName=table_arn, Records=[record])
    except Exception as e:
        print("Error writing data to Timestream:", e)

def lambda_handler(event, context):
    # Ensure that the "x_axis" and "y_axis" properties exist in the incoming data
    if 'x_axis' in event and 'y_axis' in event and 'device_id' in event and 'battery' in event:
        device_id = event['device_id']
        battery = event['battery']
        
        # Read the last readings from S3
        last_readings = read_last_readings(device_id)
        lastX = last_readings['lastX']
        lastY = last_readings['lastY']
        lastTime = last_readings['lastTime']

        if lastX is not None and lastY is not None and lastTime is not None:
            # Calculate the change in x and y axes
            delta_x = event['x_axis'] - lastX
            delta_y = event['y_axis'] - lastY

            # Calculate the time delta
            current_time = int(time.time())
            time_delta = current_time - lastTime

            # Calculate the total velocity (magnitude of the velocity vector)
            total_velocity = ((delta_x ** 2 + delta_y ** 2) ** 0.5) / time_delta

            # Convert the current time to microseconds since epoch
            current_time_micros = int(time.time() * 1000000)

            # Write the current readings to S3
            write_last_readings(event['x_axis'], event['y_axis'], device_id)

            # Write the event data to Amazon Timestream
            write_to_timestream(event['x_axis'], event['y_axis'], total_velocity, device_id, battery, current_time_micros)
            
            # Return only the specified fields in the desired format
            return {
                'x_axis': event['x_axis'],
                'y_axis': event['y_axis'],
                'totalVelocity': total_velocity,
                'device_id': device_id,
                'battery': battery,
                'time': datetime.utcfromtimestamp(current_time_micros // 1000000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
        else:
            # If last readings are not available, store the current readings and return None for total velocity
            write_last_readings(event['x_axis'], event['y_axis'], device_id)
            return {
                'x_axis': event['x_axis'],
                'y_axis': event['y_axis'],
                'totalVelocity': None,
                'device_id': device_id,
                'battery': battery,
                'time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
    else:
        raise ValueError("Invalid data format. 'x_axis', 'y_axis', 'device_id' and 'battery' properties are required.")
