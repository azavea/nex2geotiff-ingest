#!/usr/bin/env python

import os
import logging
import json
import argparse
from time import sleep

import boto.sqs
from boto.sqs.message import Message

from process_dataset import process_dataset
from nex import BASE_TIMES, ALL_MODELS, AVERAGE_MB

logger = logging.getLogger()


SQS_SLEEP_TIME = 10  # seconds to wait after getting no messages from SQS
SQS_EMPTY_CYCLES_BEFORE_EXIT = 10  # amount of cycles with no messages before exit


def get_messages(rcps, years, vars, models=ALL_MODELS):
    """
    Yields dicts to describe tasks for the worker to complete from
    lists of rcps, years, vars, and models
    """
    for rcp in rcps:
        for year in years:
            for var in vars:
                for model in models:
                    yield {'rcp': rcp, 'year': year, 'var': var, 'model': model}


def create_messages(queue, target_bucket, rcps, years, vars, models=ALL_MODELS):
    """
    For a list of rcps, years, vars, and models, assemble a list of messages
    for a worker. Present the messages to the user, and upon confirmation
    create these messages in SQS.
    """
    messages = list(get_messages(rcps, years, vars, models))
    print('\n'.join(map(str, messages)))
    print('Est. size: {} MB Okay? Type "yes" to submit job to queue.'
          .format(AVERAGE_MB * len(messages)))
    response = raw_input()
    if response.lower() == 'yes':
        for message in messages:
            message['target'] = target_bucket
            create_message(queue, json.dumps(message))


def create_message(queue, body):
    """
    Create a message in SQS with the provided body
    """
    message = Message()
    message.set_body(body)
    queue.write(message)


def handle_message(message, queue):
    """
    Process a message from SQS
    """
    message_body = json.loads(message.get_body())
    logger.info('GOT A MESSAGE! %s', str(message_body))
    try:
        logger.info('Processing message %s', str(message_body))
        process_dataset(message_body['rcp'],
                        message_body['var'],
                        message_body['model'],
                        message_body['year'],
                        message_body['target'])
        queue.delete_message(message)
        logger.info('Finished!')
    except:
        logger.error('Could not process message %s', str(message_body))


def process_queue_loop(queue):
    """
    Process messages from queue until queue is empty
    """
    miss_count = 0
    while miss_count < SQS_EMPTY_CYCLES_BEFORE_EXIT:
        message = queue.read()
        if message:
            handle_message(message, queue)
            miss_count = 0
        else:
            sleep(SQS_SLEEP_TIME)
            miss_count += 1


def main():
    """
    If WORKER_QUEUE and IS_WORKER environment variables are set,
    process messages from the queue and exit when empty.

    Otherwise, provide CLI for submitting tasks to the queue.
    """
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    queue_name = os.getenv('WORKER_QUEUE')
    aws_region = os.getenv('WORKER_REGION', 'us-east-1')

    logger.debug('KEY: %s', os.getenv('AWS_ACCESS_KEY_ID'))
    logger.debug('TARGET QUEUE: %s', queue_name)

    if os.getenv('IS_WORKER', False):
        conn = boto.sqs.connect_to_region(aws_region)
        queue = conn.get_queue(queue_name)
        process_queue_loop(queue)
        exit()

    parser = argparse.ArgumentParser(description="Submit messages to queue")
    parser.add_argument('queue', metavar='QUEUE', type=str,
                        help='Queue name')
    parser.add_argument('rcps', metavar='RCPS', type=str,
                        help='RCPs separated by comma')
    parser.add_argument('models', metavar='MODELS', type=str,
                        help='List of models, separated by comma')
    parser.add_argument('years', metavar='YEARS', type=str,
                        help='List of years, separated by comma')

    parser.add_argument('vars', metavar='VARS', type=str,
                        help='List of vars, separated by comma')
    parser.add_argument('target', metavar='TARGET', type=str,
                        help='target bucket')

    args = parser.parse_args()
    conn = boto.sqs.connect_to_region(aws_region)
    queue = conn.get_queue(args.queue)

    if args.models == 'all':
        models = BASE_TIMES.keys()
    else:
        models = args.models.split(',')

    create_messages(queue,
                    args.target,
                    args.rcps.split(','),
                    args.years.split(','),
                    args.vars.split(','),
                    models)

if __name__ == '__main__':
    main()
