#!/usr/bin/env python

"""
Code for extracting WGS84 tiles from NEX climate data. Performs the following pipeline:

1. Read the file netCDF file from S3 when a path pops in the sqs queue onto the local file system.
2. Tile that netCDF.
3. Upload to the appropriate bucket.

Note: time units is "days since 1950-01-01 00:00:00"
"""
import argparse
import os
import re
import shutil
import tempfile
import logging
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from extract_and_convert import extract

logger = logging.getLogger()


def parse_filename(path):
    """
    Returns a tuple with datatype, context, and model parts for a given
    path.
    """
    name = os.path.splitext(os.path.basename(path))[0]
    m = re.match('(?P<datatype>[^_]+)_amon_BCSD_(?P<context>[^_]+)_r1i1p1_CONUS_(?P<model>[^_]+)\_',
                 name)
    if m:
        datatype = m.group('datatype')
        context = m.group('context')
        model = m.group('model')
        return (datatype, context, model)

    m = re.match('(?P<datatype>[^_]+)_quartile75_amon_(?P<context>[^_]+)_CONUS_.*', name)
    if m:
        datatype = m.group('datatype')
        context = m.group('context')
        model = 'ensemble'
        return (datatype, context, model)

    logger.error('Could not parse name %s', name)
    return None


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


def upload_to_s3(tile_dir, datatype, context, model, target_bucket):
    conn = S3Connection()
    bucket = conn.get_bucket(target_bucket)
    for filename in os.listdir(tile_dir):
        path = os.path.join(tile_dir, filename)
        key = Key(bucket)
        key.key = '{}/{}/{}/{}'.format(context, datatype, model, filename)
        logger.info('Uploading %s to %s', filename, key.key)
        key.set_contents_from_filename(path)


def process_path(s3path, target_bucket):
    """
    Download a NetCDF file from s3, extract and convert its contents to
    geotiffs, and upload the geotiffs to a target bucket.
    """
    (s3key, path) = read_from_s3(s3path)

    try:
        parsed = parse_filename(s3key)
        if parsed:
            (datatype, context, model) = parsed
            tempdir = tempfile.mkdtemp()
            logger.info('Tiling to %s', tempdir)
            extract(path, tempdir, datatype, s3key)
            try:
                upload_to_s3(tempdir, datatype, context, model, target_bucket)
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
    parser.add_argument('s3url', metavar='S3URL', type=str,
                        help='S3 url of NETCDF to load')
    parser.add_argument('target', metavar='TARGETBUCKET', type=str,
                        help='Name of target bucket')
    args = parser.parse_args()
    process_path(args.s3url, args.target)


if __name__ == '__main__':
    main()
