from kafka import KafkaProducer
from time import sleep
from datetime import datetime
import time

if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # producer = KafkaProducer(bootstrap_servers='35.205.91.60:9092')
    #filename = 'dataset/bus-breakdown-and-delays.csv'
    filename = 'dataset/prova.csv'
    K = 1 / 4000  # compression factor
    # K = 0  # compression factor

    f = open(filename, "r")

    # read to skip the header
    f.readline()

    line_count = 0
    prev_date = 0
    for line_count, line in enumerate(f):
        if len(line.split(";")) == 21:
            date_time_str = line.split(";")[7]
            date_object1 = datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            date_c = time.mktime(date_object1.timetuple())
            if line_count == 0:
                prev_date = date_c
            sleep((date_c - prev_date) * K)
            producer.send('flink', str.encode(line))
            prev_date = date_c
        else:
            continue

        print(line_count)

    print("Lines proccessed:", line_count)

    f.close()
