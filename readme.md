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
