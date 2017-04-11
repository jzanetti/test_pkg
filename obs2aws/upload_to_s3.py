#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This module works out an appropriate s3 key and uploads a local observation
file to that key

Created on Wed Jan 11 50:41:19 2017

@author: wim
"""
import datetime
import logging
import math
import sys
import time

import numpy

from AMPSAws import resources
from . import tools

LOGGER = logging.getLogger() #use root logger (the one defined in the calling script)

def _lon_angle_eastward_between(lonwest, loneast):
    '''uses the definition of cross product to return the distance eastward from
    lonwest to loneast'''
    radwest = math.radians(lonwest)
    radeast = math.radians(loneast)
    sin_diff = math.sin(radeast) * math.cos(radwest) - math.cos(radeast) * math.sin(radwest)
    degrees = math.degrees(math.asin(sin_diff))
    return _lon_0_360(degrees)

def _lon_0_360(lon):
    if lon < 0:
        return lon + 360.0
    else:
        return lon

def get_bounding_box(messages):
    '''returns the minimum bounds which cover all of the reports in these messages '''
    north = None
    each_message = 0
    for data, report_count in messages:
        for each in range(report_count):
            try:
                lat = data['latitude'][each]
                lon = _lon_0_360(data['longitude'][each])
                if -90.0 <= lat <= 90.0:
                    if north is None:
                        north = lat
                        south = lat
                        west = lon
                        east = lon
                    else:
                        north = max(north, lat)
                        south = min(south, lat)
                        if lon < west or lon > east:
                            dist_west = _lon_angle_eastward_between(lon, west)
                            dist_east = _lon_angle_eastward_between(east, lon)
                            if dist_east < dist_west:#extend eastern bound
                                east = lon
                            else:#extend western bound
                                west = lon
                            if east < west:
                                east += 360.0
            except (IndexError, KeyError, ValueError, numpy.ma.MaskError):
                LOGGER.debug('problem reading lat or long [%d] in message[%s]',
                             each, each_message)
        each_message += 1
    if north is not None:
        west = tools.lon_180_180(west)
        east = tools.lon_180_180(east)
        return {'s':south, 'n':north, 'w':west, 'e':east}
    else:
        return {}

def get_nominal_times(messages, timestamp, received):
    '''Returns a tuple of (valid_from, valid_to) from all of the reports in these messages.
    Fall back to the timestamp in the filename for both valid_from and valid_to
     if all else fails'''
    valid_from = None
    valid_to = None
    each_message = 0
    for data, report_count in messages:
        has_second = 'second' in data
        for each in range(report_count):
            try:
                if 'datetime' in data:
                    if len(data['datetime']) > each:
                        valid_time = datetime.datetime.strptime(data['datetime'][each],
                                                                '%Y%m%d%H%M%S')
                    else:
                        #we have already read all of the datetimes that we have
                        break
                else:
                    if has_second:
                        second = data['second'][each]
                    else:
                        second = 0
                    valid_time = datetime.datetime(data['year'][each],
                                                   data['month'][each],
                                                   data['day'][each],
                                                   data['hour'][each],
                                                   data['minute'][each],
                                                   second)
                if valid_from is None:
                    valid_from = valid_time
                    valid_to = valid_time
                elif valid_time < valid_from:
                    valid_from = valid_time
                elif valid_time > valid_to:
                    valid_to = valid_time
            except (IndexError, KeyError, ValueError, numpy.ma.MaskError):
                LOGGER.debug('problem reading time of report[%d] in message[%s]',
                             each, each_message)

        each_message += 1
    if valid_from is None:
        #work out a default time from the filename.  Typically this is when the bulletin
        # was sent, so it may be quite a bit later than the validity of the reports.
        valid_from = datetime.datetime.strptime(timestamp, '%Y%m%d%H%M%S')
        valid_to = valid_from
    return {'valid_from':valid_from, 'valid_to':valid_to, 'received':received}

def get_s3key(s3_base, report_type, gts_header, file_ext, bounding_box, times):
    '''some of this is in the configuration and the remainder is derived from the filename
    General form is
    "s3://<base_path>type/yyyy/mm/dd/hh/validfrom_validto_received_bbox_gtsheader"
      where yyyy, mm, dd, hh, MMS refer to the year, month, day, hour, minute
        repectively of validfrom.
      validfrom is the time of the earliest report and is of the form yyyymmddHHMMSS
      validto (latest report) is of the form yyyymmddHHMMSS
      received is of the form yyyymmddHHMMSS
      bbox is of the form "south_west_north_east"

    Example
    For a bulletin received exactly at 7 minutes past 0000UTC on 9 Nov 2016
    s3://metservice-research-us-west-2/synop/2016/11/08/22/20161108223600_
         20161108230100_20161109000700_-50_160-29.5_-175_ISNK12_AMMC_RRI.bfr'''

    if s3_base.endswith('/') and not s3_base.endswith('//'):
        first_part = s3_base[:-1]
    else:
        first_part = s3_base

    LOGGER.debug('s3_base:%s', s3_base)

    final_part = '_'.join([
        times['valid_from'].strftime('%Y%m%d%H%M%S'),
        times['valid_to'].strftime('%Y%m%d%H%M%S'),
        times['received'].strftime('%Y%m%d%H%M%S'),
        round2tenths(bounding_box['s'], False),#south
        round2tenths(bounding_box['w'], False),#west
        round2tenths(bounding_box['n'], True),#north
        round2tenths(bounding_box['e'], True),#east
        gts_header])
    if file_ext:
        final_part = final_part + '.' + file_ext

    return '/'.join([first_part,
                     report_type,
                     times['valid_from'].strftime('%Y'), #year
                     times['valid_from'].strftime('%m'), #month
                     times['valid_from'].strftime('%d'), #day
                     times['valid_from'].strftime('%H'),
                     final_part]) #hour

def round2tenths(number, round_up):
    '''round a number to nearest tenth and return a string. For bounding boxes'''
    is_negative = number < 0.0
    absnumber = abs(number)
    tenths_as_int = int(absnumber*10)
    hundredths = int(absnumber*100) % 10
    if hundredths > 0:
        if round_up:
            tenths_as_int += 1
        elif is_negative: #and not rounding up
            tenths_as_int += 1
    if is_negative:
        tenths_as_int = -tenths_as_int
    return '%.1f' %(tenths_as_int / 10.0)

def s3copy(source, destination, role_name, keys_path, s3_options):
    '''do the resources.copy thing, with a 2 attempt retry loop'''
    attempts = 2
    sleep_seconds = 0.05

    LOGGER.debug('writing %s to %s', source, destination)
    while attempts > 0:
        try:
            resources.copy(source,
                           destination,
                           keys_path=keys_path,
                           role_name=role_name,
                           s3options=s3_options)
            LOGGER.info('wrote %s to %s', source, destination)
            return
        except Exception:
            attempts -= 1
            LOGGER.warning('to %s failed remaining attempts: %i - %s',
                           destination, attempts, sys.exc_info())
            time.sleep(sleep_seconds)
            sleep_seconds *= 2 #exponential backoff
    LOGGER.error('failed to copy %s to %s', source, destination)
    raise
