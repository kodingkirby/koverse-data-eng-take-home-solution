# koverse assessment
```
pip install -r requirements.txt
```

Next, set up credentials (in e.g. ~/.aws/credentials):
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET

or run aws config if you have the AWS CLI installed

start random data generator:
```
python3 generate-flights.py &

```
then open another terminal window and start loader:
```
python3 loader.py
```
prompt:
A Raspberry Pi is connected to sensors and running software that allows it to capture the position of aircraft flying nearby. About every 1 second, the software on the device re-writes a file /data/aircraft.json on the filesystem. This file represents the current state of all visible aircraft at a point in time. The aircraft.json file contains an array of objects, one for each aircraft seen. The file looks like

{ "now" : 1622691156012,
  "aircraft" : [
{"hex":"a21d14","flight":"SWA1678","alt_geom":10475,"gs":295.1,"track":91.2,"lat":39.874878,"lon":-104.454186},
{"hex":"a6b3aa","flight":"N5306U","alt_geom":7100,"gs":94.9,"track":277.9,","lat":39.553574,"lon":-104.896326}
…
]}

 For each object in the aircraft array, there is an altitude (alt_geom), latitude (lat), and longitude (lon). The timestamp associated with these positions is seen at the top-level of the file in the now property. There are on the order of 100-300 objects in the array.

Your task is to get this near real-time data from the device into a database of your choice. The only requirement is the database cannot be running on the Raspberry Pi device. The database can be hosted in the cloud, it can be MongoDB or Postgres or DynamoDB - it is your choice. Each row or document in the database should represent the position of a single aircraft at a point in time. The database should be appended to as the position/timestamps of the aircraft change. How applications will use the data in the database is outside the scope of this question, so focus on loading the data only.

You are free to use any software on the device or any cloud services you chose. Please describe the architecture of the data flow you would use to solve this problem. Your response should be limited to a maximum of 2 pages and should include diagrams and text. We are looking for a notional design, not an actual implementation, and thus there may be details unaccounted for, and that is OK. Our expectation is that you spend no more than an hour or so on your response.

There is no right answer to this question. We are evaluating answers based on how well they communicate a proposed solution. Dimensions such as cost, security, engineering effort, and complexity, are some of the things we think would be interesting to discuss as part of your proposed architecture. Feel free to just state any assumptions.
