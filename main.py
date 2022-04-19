from kafka import KafkaProducer, KafkaClient, KafkaConsumer
from time import sleep
import json, uuid
import threading


KAFKA_HOST = 'localhost:9092'


def producing():
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
    for i in range(10):
        mess_id = str(uuid.uuid4())
        mess = {
            'request_id': mess_id,
            'data': i,
        }
        producer.send('app_message', json.dumps(mess).encode('utf-8'))
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(mess_id))
        sleep(2)


def consuming():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST)
    for msg in consumer:
        mess = json.loads(msg.value)
        request_id = mess['request_id']
        print("\033[1;32;40m ** CONSUMER: Received prediction {} for request id {}".format(mess['data'],
                                                                                           request_id))


if __name__ == '__main__':
    threads = []
    t1 = threading.Thread(target=producing)
    t2 = threading.Thread(target=consuming)
    threads.append(t1)
    threads.append(t2)
    t1.start()
    t2.start()


