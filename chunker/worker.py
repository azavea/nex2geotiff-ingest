import os
import logging
import json
from time import sleep
from process_s3_path import process_path

import boto.sqs
from boto.sqs.message import Message

logger = logging.getLogger()

AVERAGE_MB = 796000000 / 1024 / 1024
ALL_MODELS = ['ACCESS1-0',
              'BNU-ESM',
              'CCSM4',
              'CESM1-BGC',
              'CNRM-CM5',
              'CSIRO-Mk3-6-0',
              'CanESM2',
              'GFDL-CM3',
              'GFDL-ESM2G',
              'GFDL-ESM2M',
              'IPSL-CM5A-LR',
              'IPSL-CM5A-MR',
              'MIROC-ESM-CHEM',
              'MIROC-ESM',
              'MIROC5',
              'MPI-ESM-LR',
              'MPI-ESM-MR',
              'MRI-CGCM3',
              'NorESM1-M',
              'bcc-csm1-1',
              'inmcm4']


def get_key(rcp, year, datatype, model):
    FORMAT = ('s3://nasanex/NEX-GDDP/BCSD/{rcp}/day/atmos/{datatype}/r1i1p1/v1.0/'
              '{datatype}_day_BCSD_{rcp}_r1i1p1_CONUS_{model}_{year}.nc')
    return FORMAT.format(model=model, rcp=rcp, datatype=datatype, year=year)


def get_keys(rcps, years, datatypes, models=ALL_MODELS):
    for rcp in rcps:
        for year in years:
            for datatype in datatypes:
                for model in models:
                    yield get_key(rcp, year, datatype, model)


def create_messages(target_bucket, rcps, years, datatypes, models=ALL_MODELS):
    keys = list(get_keys(rcps, years, datatypes, models))
    print('\n'.join(keys))
    print('Est. size: {} MB Okay? Type "yes" to submit job to queue.'
          .format(AVERAGE_MB * len(keys)))
    response = raw_input()
    if response.lower() == 'yes':
        for 


def create_message(queue, s3key, target_bucket):
    message = Message()
    message.set_body(json.dumps({'s3key': s3key, 'target_bucket': target_bucket}))
    queue.write(message)


def handle_message(message, queue):
    message = json.loads(message.get_body())
    logger.info('GOT A MESSAGE! %s to %s', message['s3key'], message['target_bucket'])
    try:
        logger.info('Processing path %s', message['s3key'])
        process_path(message['s3key'], message['target_bucket'])
        queue.delete_message(message)
        logger.info('Finished!')
    except:
        logger.error('Could not process path %s', message['s3key'])


def process_queue_loop(queue):
    miss_count = 0
    while miss_count < 10:
        message = queue.read()
        if message:
            handle_message(message, queue)
        else:
            miss_count += 1


def main():
    queue_name = os.getenv('WORKER_QUEUE')
    aws_region = os.getenv('WORKER_REGION', 'us-east-1')

    logger.debug('KEY: %s', os.getenv['AWS_ACCESS_KEY_ID'])

    logger.debug('TARGET QUEUE: %s', queue_name)

    conn = boto.sqs.connect_to_region(aws_region)
    queue = conn.get_queue(queue_name)
    process_queue_loop(queue)
