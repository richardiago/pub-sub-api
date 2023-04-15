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

semaphore = threading.Semaphore(1)
latest_replay_id = None

def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)
        

def decode(schema, payload):
  schema = avro.schema.parse(schema)
  buf = io.BytesIO(payload)
  decoder = avro.io.BinaryDecoder(buf)
  reader = avro.io.DatumReader(schema)
  ret = reader.read(decoder)
  return ret


with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())

with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:

    sessionid = '00D8b0000020N8y!AQYAQIzAwcJWUa72nZKZPon5zkW0MiTqaoX_.ZWOC7eLYXahceFKo7kefYOorelsqxN55fj6o2oPBDob2dwU80AwaNc7ILbN'
    instanceurl = 'https://resourceful-badger-a8o4n1-dev-ed.trailblaze.my.salesforce.com'
    tenantid = '00D8b0000020N8yEAE'

    authmetadata = (('accesstoken', sessionid), ('instanceurl', instanceurl), ('tenantid', tenantid))

    stub = pb2_grpc.PubSubStub(channel)

    mysubtopic = "/event/Nebula__LogEntryEvent__e"
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
    