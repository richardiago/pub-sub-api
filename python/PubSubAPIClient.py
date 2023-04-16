from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
import os

semaphore = threading.Semaphore(1)

def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(topic_name = topic, replay_preset = pb2.ReplayPreset.LATEST, num_requested = 1)

def decode(schema, payload):
  
  schema  = avro.schema.parse(schema)
  buf     = io.BytesIO(payload)
  decoder = avro.io.BinaryDecoder(buf)
  reader  = avro.io.DatumReader(schema)
  ret     = reader.read(decoder)

  return ret

def getSalesforceOAuthInfo():

    auth_url = os.environ['SALESFORCE_AUTH_URL']
    auth_data = {
        'grant_type': 'password',
        'client_id': os.environ['SALESFORCE_CLIENT_ID'],
        'client_secret': os.environ['SALESFORCE_CLIENT_SECRET'],
        'username': os.environ['SALESFORCE_USERNAME'],
        'password': os.environ['SALESFORCE_PASSWORD']
    }

    auth_response = requests.post(auth_url, data=auth_data)
    auth_response_json = auth_response.json()

    sessionid = auth_response_json['access_token']
    instanceurl = auth_response_json['instance_url']
    tenantid = auth_response_json['id'].split('/')[4]

    return sessionid, instanceurl, tenantid


def pubSubClient():

    # semaphore = threading.Semaphore(1)
    latest_replay_id = None

    with open(certifi.where(), 'rb') as f:
        creds = grpc.ssl_channel_credentials(f.read())

    with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:

        sessionid, instanceurl, tenantid = getSalesforceOAuthInfo()

        authmetadata = (('accesstoken', sessionid), ('instanceurl', instanceurl), ('tenantid', tenantid))

        stub = pb2_grpc.PubSubStub(channel)

        mysubtopic = os.environ['SALESFORCE_EVENT']
        print('Subscribing to ' + mysubtopic)
        substream = stub.Subscribe(fetchReqStream(mysubtopic), metadata=authmetadata)

        for event in substream:

            if event.events:

                semaphore.release()
                print("Number of events received: ", len(event.events))
                payloadbytes = event.events[0].event.payload
                schemaid = event.events[0].event.schema_id
                schema = stub.GetSchema(pb2.SchemaRequest(schema_id=schemaid), metadata=authmetadata).schema_json
                decoded = decode(schema, payloadbytes)
                print("Got an event!", json.dumps(decoded))

            else:
                print("[", time.strftime('%b %d, %Y %l:%M%p %Z'), "] The subscription is active.")

        latest_replay_id = event.latest_replay_id

def lambda_handler(event, context):
    pubSubClient()