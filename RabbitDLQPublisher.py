import time
from datetime import datetime
import json
import pandas as pd
import requests
import base64

def save_republished_messages(messages, file_path):
    with open(file_path, 'x') as file:
        for message in messages:
            file.write(message + '\n')

def read_already_republished_messages(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            return lines
    except FileNotFoundError:
        print(f"File '{file_path}' not found. Setting lines [].")
        lines = []
        return lines

def publish(msg_payload, rabbit_port, exchange, routing_key, username, password):
    credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    url = f"http://localhost:{rabbit_port}/api/exchanges/%2F/{exchange}/publish"
    headers = {"Content-Type": "application/json",
               "Authorization": f"Basic {credentials}"
               }
    payload = {
        "vhost": "/",
        "name": f"{exchange}",
        "properties": {
            "delivery_mode": 2,
            "headers": {}
        },
        "routing_key": f"{routing_key}",
        "delivery_mode": "2",
        "payload": f'{msg_payload}',
        "payload_encoding": "string",
        "headers": {},
        "props": {}
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code // 100 == 2:
            print(f"Message {msg_payload} published sucessfully.")
            return "ok"
        else:
            print(f"Failed to publish message {msg_payload}")
            return "failed"
    except requests.RequestException as e:
        print(f"An error occurred: {e}")

start_date = int(datetime.strptime(input("The initial date that messages started failing (YYYY-MM-DDTHH:MM): "), '%Y-%m-%dT%H:%M').timestamp())
end_date = int(datetime.strptime(input("The end date to limit the interval for messages that will be republished (YYYY-MM-DDTHH:MM): "), '%Y-%m-%dT%H:%M').timestamp())
routing_key = input(
    "Provide the Routing key for the event (normally the Event object path in application source code: ")
delay_between_messages = int(input("Republishing messages delay between them: ")) # seconds for avoiding CPU peak
first_death_reason = "rejected"
messages_file_path = input(
    "Provide the relative path from this script location to the file with messages to republish in JSON format: ")
rabbit_port = input("Port for RabbitMQ on your machine: ")
republished_msgs_file_path = input(
    "File path that has history for all republished messages about for this context IF already exist (if not, "
    "just press Enter)")
username = input("Type RabbitMQ username: ")
password = input("Type RabbitMQ password: ")

print("Starting message republication for event consumer...")

with open(messages_file_path, 'r') as json_file:
    data = json.load(json_file)

df = pd.json_normalize(data)
df = df.iloc[::-1].reset_index(drop=True)

republished_messages = read_already_republished_messages(republished_msgs_file_path)
for index, row in df.iterrows():
    if row['routing_key'] == routing_key and row['properties.headers.x-first-death-reason'] == 'rejected' and start_date <= row['properties.headers.x-death'][0].get('time') <= end_date:
        is_republished = False
        for line in republished_messages:
            if str(row['properties.message_id']) in line:
                is_republished = True
                break

        if is_republished is False:
            result = publish(row['payload'], rabbit_port, row['properties.headers.x-first-death-exchange'], row['routing_key'], username, password)

            if result == "ok":
                republished_messages.append(str(row['properties.message_id']))
            time.sleep(delay_between_messages)

history_file_path = f"./Republished--{routing_key}--{datetime.now().strftime('%Y-%m-%dT%H:%M')}.txt"
save_republished_messages(republished_messages, history_file_path)

print(f"Done! Republished message IDs saved in {history_file_path}")