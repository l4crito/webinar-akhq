import os, sys
from confluent_kafka import (Consumer, KafkaError, KafkaException)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


def msg_process(msg):
    print('User record {} successfully consumed to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

running = True

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

def main(argv):
    topic = 'PEINT.KafkaClientPythonExample.v1'
    #broker_url
    bootstrap_servers = os.environ['BOOTSTRAP_SERVER']
    cacert_path = '/var/task/cacert/graingerchain.pem'

    consumer_conf = {'bootstrap.servers': bootstrap_servers
        ,'security.protocol': 'SSL'
        ,'ssl.ca.location': cacert_path
        ,'ssl.certificate.location': os.environ['MSK_CERTIFICATE_PATH'] #path to certificate pem file
        ,'ssl.key.location': os.environ['MSK_PRIVATEKEY_PATH'] #path to pivate key pem file
        ,'group.id': "foo"
        ,'auto.offset.reset': 'earliest'
                     }

    consumer = Consumer(consumer_conf)

    print("Consume user records to topic {}.".format(topic))

    consume_loop(consumer,[topic])


if __name__ == "__main__":
    main(sys.argv[1:])
