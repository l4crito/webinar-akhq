from uuid import uuid4
import ssl
import sys
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import json
import os, sys, getopt
from datetime import datetime
import time

class PeintRecord(object):
    """
    PeintRecord record
    Args:
        domain_name (str): PeintRecord's domain_name
        domain_address (str): PeintRecord's domain_address
        run_dt (str): PeintRecord's run_dt
    """
    def __init__(self, domain_name, domain_address, run_dt):
        self.domain_name = domain_name
        self.domain_address = domain_address
        self.run_dt = run_dt


def record_to_dict(record, ctx):
    """
    Returns a dict representation of a PeintRecord instance for serialization.
    Args:
        record (PeintRecord): PeintRecord instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    # User._address must not be serialized; omit from dict
    return dict(domain_name=record.domain_name,
                domain_address=record.domain_address,
                run_dt=record.run_dt)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(argv):
    topic = 'PEINT.KafkaClientPythonExample.v1'
    #broker_url
    bootstrap_servers = 'localhost:9092'
    #schema_registry_url
    schema_registry =  'http://localhost:8081'
    #cacert_path = '/var/task/cacert/graingerchain.pem'

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "S3toTD",
      "description": "A Confluent Kafka S3 to TD",
      "type": "object",
      "properties": {
        "domain_name": {
          "description": "Name of the Domain",
          "type": "string"
        },
        "domain_address": {
          "description": "Exact location",
          "type": "string"
        },
        "run_dt": {
          "description": "Run date",
          "type": "string"
        }
      },
      "required": [ "domain_name", "domain_address"]
    }
    """

    #schema_registry_conf = {'url': schema_registry, 'ssl.ca.location':cacert_path}
    schema_registry_conf = {'url': schema_registry}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, record_to_dict)

    producer_conf = {'bootstrap.servers': bootstrap_servers
        ,'key.serializer': StringSerializer('utf_8')
        ,'value.serializer': json_serializer
     }

    producer = SerializingProducer(producer_conf)

    print("Producing user records to topic {}.".format(topic))

    #lazo para continuar produciendp de forma indefinida
    while(True):
        producer.poll(0.0)
        try:
            domain_name = "WISP"
            domain_address = "OCCUPANT"
            run_dt = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            terarecord = PeintRecord(domain_name,domain_address, run_dt)

            print("Before-----")
            producer.produce(topic=topic, key=str(uuid4()), value=terarecord,
                             on_delivery=delivery_report)
            print("After-----")
            time.sleep(1)
        except Exception as e:
            print(e)

        print("\nFlushing records...")
        producer.flush()


if __name__ == "__main__":
    main(sys.argv[1:])
