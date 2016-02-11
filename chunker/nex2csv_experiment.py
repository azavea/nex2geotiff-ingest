#!/usr/bin/env python

import os
from datetime import datetime, timedelta
import logging
import json

import argparse

import numpy

os.environ['GDAL_NETCDF_BOTTOMUP'] = 'NO'
import rasterio
from rasterio._io import RasterReader

logger = logging.getLogger()

DATE_FORMAT = '%Y%m%d%H%M%S'


def get_cities():
    """
    Loads cities.json and yields tuples with the
    city name
    country
    x coord
    y coord
    """
    with open('cities.json', 'r') as f:
        citydata = json.load(f)
    for city in citydata['features']:
        yield (city['properties']['name'],
               city['properties']['admin'],
               city['geometry']['coordinates'][0],
               city['geometry']['coordinates'][1])


def open_netCDF(path, subds=''):
    """
    Opens a NetCDF file as a rasterio object
    """
    p = 'NETCDF:' + path
    if subds:
        p += ':' + subds
    s = RasterReader(p)
    s.start()
    return s


def get_window(affine, x, y, height):
    # window = ((y2, y1), (x1, x2)) because rows get read backwards
    (col, row) = ~affine * (x, y)
    col, row = int(col), int(row)
    return ((height - row, height - row + 1), (col, col + 1))


def nex2json(input_path,
            out_dir,
            subds='',
            s3key='',
            year=2006):
    """
    Extracts and converts tiles from NetCDF to geotiff

    Arguments:
    input_path -- the path of the NetCDF file to extract from
    subds -- the NetCDF subdataset to extract from
    s3key -- the S3 key the NetCDF is from, to be embedded in the metadata
    base_time -- time from which NetCDF time tag starts counting from
    target_cols - width of target tile size
    target_rows - height of target tile size
    """
    with rasterio.drivers():
        with open_netCDF(input_path, subds) as dataset:
            cols = dataset.meta['width']
            rows = dataset.meta['height']

            # Logic for doing extent windowing.
            affine = dataset.affine

            days_since = int(float(dataset.tags(1)['NETCDF_DIM_time']))
            base_time = datetime(year, 1, 1) - timedelta(days=days_since)
            print base_time

            output = []

            for name, admin, x, y in get_cities():
                citydata = {'name': name,
                            'admin': admin,
                            subds: []}
                read_window = get_window(affine, x, y, rows)

                for i in range(1, dataset.count + 1):
                    tags = dataset.tags(i)
                    days_since = float(tags['NETCDF_DIM_time'])
                    band_date = base_time + timedelta(days=days_since)
                    band_date_name = band_date.strftime(DATE_FORMAT)

                    # WEIRDNESS: The netCDF is "bottom-up" data. This causes GDAL
                    # to not be able to work with it unless this evnironment variable
                    # is exported: export GDAL_NETCDF_BOTTOMUP=NO
                    # So it reads the band upside down, and we need to flip it.
                    wrong_way_up = dataset.read_band(i, window=read_window)
                    tile_data = numpy.flipud(wrong_way_up)
                    citydata[subds].append((band_date_name, float(tile_data[0][0])))
                output.append(citydata)
    with open('output.json', 'w') as f:
        json.dump(output, f)




def main():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description='Converts netcdf to geotiff')
    parser.add_argument('infile', metavar='INFILE', type=str,
                        help='The input NETCDF')
    parser.add_argument('subdataset', metavar='SUBDS', type=str,
                        help='Subdataset ID')
    parser.add_argument('id', metavar='ID', type=str,
                        help='s3 ID')
    parser.add_argument('year', metavar='YEAR', type=int,
                        help='year')
    args = parser.parse_args()
    nex2json(args.infile, './', args.subdataset, args.id, args.year)


if __name__ == '__main__':
    main()
