#!/usr/bin/python
import time
import datetime
import pytz
import numpy
import random
import gzip
import zipfile
import sys
import argparse
import os
from faker import Faker
from random import randrange
from pytz import timezone
import pytz
from kafka import KafkaProducer

import os

local = timezone('Europe/Amsterdam')


# todo:
# allow writing different patterns (Common Log, Apache Error log etc)
# log rotation


class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args:  # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False


parser = argparse.ArgumentParser(__file__, description="Fake Apache Log Generator")
parser.add_argument("--output", "-o", dest='output_type', help="Write to a Log file, a gzip file or to STDOUT", choices=['LOG', 'GZ', 'CONSOLE', 'KAFKA'], default="KAFKA")
parser.add_argument("--log-format", "-l", dest='log_format', help="Log format, Common or Extended Log Format ", choices=['CLF', 'ELF'], default="ELF")
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate by batch", type=int, default=os.getenv('LOGGEN_NUM', 50))
parser.add_argument("--prefix", "-p", dest='file_prefix', help="Prefix the output file name", type=str)
parser.add_argument("--sleep", "-s", help="Sleep this long between lines (in seconds)",  default=os.getenv('LOGGEN_SLEEP', 0.1), type=float)
parser.add_argument("--kafka-brokers", "-k", dest='kafka_brokers', help="the kafka brokers connection string", default=os.getenv('LOGGEN_KAFKA', "kafka:9092"), type=str)
parser.add_argument("--kafka-topic", "-t", dest='kafka_topic', help="the kafka topic to publich logs on", default=os.getenv('LOGGEN_KAFKA_TOPIC', "logisland_raw"), type=str)


args = parser.parse_args()

log_lines = args.num_lines
file_prefix = args.file_prefix
output_type = args.output_type
log_format = args.log_format

faker = Faker()

timestr = time.strftime("%Y%m%d-%H%M%S")
otime = datetime.datetime.now()


response = ["200", "404", "500", "301"]

verb = ["GET", "POST", "DELETE", "PUT"]

resources = ["/list", "/wp-content", "/wp-admin", "/explore", "/search/tag/list", "/app/main/posts",
             "/posts/posts/explore", "/apps/cart.jsp?appID="]

frequent_ips = ["12.13.14.15", "123.124.125.126"]

ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]


# wait a little while zk & kafka start
time.sleep(10)
producer = KafkaProducer(bootstrap_servers=args.kafka_brokers, client_id='loggen')





def create_log():
    ip = faker.ipv4()
    if random.randint(0, 100) < 70:
        ip = numpy.random.choice(frequent_ips, p=[0.6, 0.4])
        print(ip)

    dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
    tz = datetime.datetime.now(local).strftime('%z')
    vrb = numpy.random.choice(verb, p=[0.6, 0.1, 0.1, 0.2])

    uri = random.choice(resources)
    if uri.find("apps") > 0:
        uri += str(random.randint(1000, 10000))

    resp = numpy.random.choice(response, p=[0.9, 0.04, 0.02, 0.04])
    byt = int(random.gauss(5000, 50))
    referer = faker.uri()
    useragent = numpy.random.choice(ualist, p=[0.5, 0.3, 0.1, 0.05, 0.05])()
    if log_format == "CLF":
        return '{} - - [{} {}] "{} {} HTTP/1.0" {} {}'.format(ip, dt, tz, vrb, uri, resp, byt)
    elif log_format == "ELF":
        return '{} - - [{} {}] "{} {} HTTP/1.0" {} {} "{}" "{}"'.format(ip, dt, tz, vrb, uri, resp, byt, referer, useragent)




while True:
    if args.sleep:
        increment = datetime.timedelta(seconds=args.sleep)
    else:
        increment = datetime.timedelta(seconds=random.randint(30, 300))
    otime += increment

    for x in range(0, random.randint(0, log_lines)):
        log = create_log()
        producer.send(args.kafka_topic, str.encode(log))

    if args.sleep:
        time.sleep(args.sleep)