#!/usr/bin/env python

"""
Code for extracting data about cities from NEX climate data.
Performs the following pipeline:

1. Read the file netCDF file from S3
2. Run nex2json on the dataset
3. Upload result to the appropriate bucket

"""
import argparse
import os
import re
import shutil
import tempfile
import logging
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from nex2json import nex2json
from nex import BASE_TIMES

logger = logging.getLogger()


def generate_s3_path(rcp, var, model, year):
    FORMAT = ('s3://nasanex/NEX-GDDP/BCSD/{rcp}/day/atmos/{var}/r1i1p1/'
              'v1.0/{var}_day_BCSD_{rcp}_r1i1p1_{model}_{year}.nc')
    return FORMAT.format(rcp=rcp, var=var, model=model, year=year)


def read_from_s3(s3path):
    """
    Downloads a NetCDF file from s3

    Returns a tuple with the s3 key name and the destination
    """
    m = re.match('s3://([^/]+)/(.+)', s3path)
    if m:
        bucket_name = m.group(1)
        key_name = m.group(2)
        conn = S3Connection()
        bucket = conn.get_bucket(bucket_name)
        key = bucket.get_key(key_name)
        (handle, file_path) = tempfile.mkstemp(suffix='.nc')
        logger.info('Saving to {}'.format(file_path))
        with os.fdopen(handle, 'wb') as tmp:
            key.get_file(tmp)
        return (key.name, file_path)
    else:
        logger.error('ERROR: cannot parse s3key %s', s3path)
        return None


def upload_to_s3(data_dir, var, rcp, model, target_bucket):
    """
    Uploads a directory to s3, prepending the rcp, var and model
    to the s3 key as a path
    """
    conn = S3Connection()
    bucket = conn.get_bucket(target_bucket)
    for filename in os.listdir(data_dir):
        path = os.path.join(data_dir, filename)
        key = Key(bucket)
        key.key = '{}/{}/{}/{}'.format(rcp, var, model, filename)
        logger.info('Uploading %s to %s', filename, key.key)
        key.set_contents_from_filename(path)


def process_dataset(rcp, var, model, year, target_bucket):
    """
    Download a NetCDF file from s3, extract and convert its contents to
    json, and upload the json to a target bucket.
    """
    s3path = generate_s3_path(rcp, var, model, year)
    (s3key, path) = read_from_s3(s3path)
    s3basename = os.path.splitext(os.path.basename(s3key))[0]

    try:
        tempdir = tempfile.mkdtemp()
        logger.info('Tiling to %s', tempdir)
        nex2json(path, tempdir, var, s3basename, model, BASE_TIMES[model])
        try:
            upload_to_s3(tempdir, var, rcp, model, target_bucket)
        finally:
            logger.info('Deleting directory %s', tempdir)
            shutil.rmtree(tempdir)
    finally:
        logger.info('Deleting %s', path)
        os.remove(path)


def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('rcp', metavar='RCP', type=str,
                        help='rcp45 or rcp85')
    parser.add_argument('var', metavar='VAR', type=str,
                        help='pr, tasmax, or tasmin')
    parser.add_argument('model', metavar='MODEL', type=str,
                        help='model name')
    parser.add_argument('year', metavar='YEAR', type=int,
                        help='year')
    parser.add_argument('target', metavar='TARGET', type=str,
                        help='target bucket')
    args = parser.parse_args()
    process_dataset(args.rcp, args.var, args.model, args.year, args.target)


if __name__ == '__main__':
    main()
