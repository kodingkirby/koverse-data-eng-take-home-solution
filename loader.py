import json
from time import sleep
from threading import Timer
import boto3
from decimal import Decimal
from io import BytesIO
import logging
import os
from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

#run timer in seperate thread
class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

class Flights:
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store flight data.
        The table uses the hex identifier as the partition key and the
        timestamp as the sort key.
        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'hex', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'hex', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 300})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def write_batch(self, flightrecords):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.
        :param flight: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for record in flightrecords:
                    writer.put_item(Item=record)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def add_flight(self, hex, flight, alt_geom, gs, track, lat, lon, timestamp):
        try:
            self.table.put_item(
                Item={
                    'hex': hex,
                    'flight': flight,
                    'alt_geom': alt_geom,
                    'gs': gs,
                    'track': track,
                    'lat': lat,
                    'lon': lon,
                    'timestamp': timestamp
                    })
        except ClientError as err:
            logger.error(
                "Couldn't add flight %s to table %s. Here's why: %s: %s",
                hex, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_flight(self, hex, timestamp):
        try:
            response = self.table.get_item(Key={'hex': hex, 'timestamp': timestamp})
        except ClientError as err:
            logger.error(
                "Couldn't get flight %s from table %s. Here's why: %s: %s",
                hex, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    def query_flights(self, timestamp):
        """
        Queries for flights that were released in the specified time.
        :param timestamp: The timestamp to query.
        :return: The list of flights that occured in the specified time.
        """
        try:
            response = self.table.query(KeyConditionExpression=Key('timestamp').eq(timestamp))
        except ClientError as err:
            logger.error(
                "Couldn't query for flights at %s. Here's why: %s: %s", timestamp,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

#fetch from json file
def get_flight_data(flight_file_name, flights):
    print(f"\nReading data from '{flight_file_name}' into your table.")
    try:
        with open(flight_file_name) as file:
            aircraft = json.load(file, parse_float=Decimal)
    except FileNotFoundError:
        print(f"File {flight_file_name} not found")
        raise
    else:
        timestamp = aircraft['now']
        #append timestamp to each record
        for i in aircraft['aircraft']:
            i['timestamp'] = timestamp
        flights.write_batch(aircraft['aircraft'])
        print(f"\nWrote {len(aircraft['aircraft'])} flights into {flights.table.name}.")

def run_import(table_name, flight_file_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    print("Starting script")

    flights = Flights(dyn_resource)
    flights_exists = flights.exists(table_name)
    if not flights_exists:
        print(f"\nCreating table {table_name}...")
        flights.create_table(table_name)
        print(f"\nCreated table {flights.table.name}.")
    rt = RepeatedTimer(1, get_flight_data, flight_file_name, flights) # 
    try:
        sleep(3600) # run for 1 hr
    finally:
       rt.stop()

if __name__ == "__main__":
   try:
        run_import('flights', 'flights.json', boto3.resource('dynamodb'))
   except Exception as e:
        print(f"Something went wrong! Here's what: {e}")
