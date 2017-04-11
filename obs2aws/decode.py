#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This module can decode observations in the following formats:
    BUFR
    METAR text (WMO FM 15–XIV METAR)

The following types of reports are supported:
    SYNOP, SYNOP, METAR, PILOT, TEMP, RADARVVP, SHIP, BUOY, AMDAR, ASCAT, AMV

Created on Wed Jan 11 14:41:19 2017

@author: wim
"""
import datetime
import logging
import os

import eccodes
import numpy

from obs2aws import tools
import station_details

LOGGER = logging.getLogger()


def bufr_typical_timestamp(msgid):
    '''gets the typical datetime from section1 of a BUFR message and returns as
    a masked numpy array'''
    try:
        date = datetime.datetime(eccodes.codes_get(msgid, 'typicalYear'),
                                 eccodes.codes_get(msgid, 'typicalMonth'),
                                 eccodes.codes_get(msgid, 'typicalDay'),
                                 eccodes.codes_get(msgid, 'typicalHour'),
                                 eccodes.codes_get(msgid, 'typicalMinute'),
                                 eccodes.codes_get(msgid, 'typicalSecond'))
        result = [date.strftime('%Y%m%d%H%M%S')]
        LOGGER.debug('bufr_typical_timestamp: %s -> %s', str(date), str(result))
        return numpy.ma.asarray(result)
    except (ValueError, eccodes.CodesInternalError):
        LOGGER.warning('could not get typical date')
        return None

def decode_bufr_array(msgid, key):
    '''Returns a numpy maksed array of values corresponding to the fieldname.
    None if there is an error.
    '''
    if key == 'datetime':
        return bufr_typical_timestamp(msgid)
    try:
        arr = eccodes.codes_get_array(msgid, key)
        if len(arr) == 0:
            return None
        dtype = eccodes.codes_get_native_type(msgid, key)
        if dtype is str:
            arr = numpy.char.strip(arr)
        if not isinstance(arr, numpy.ndarray):
            values = numpy.asarray(arr, dtype=dtype)
        else:
            values = arr
        if dtype is int:
            result = numpy.ma.masked_values(values, eccodes.CODES_MISSING_LONG)
        elif dtype is float:
            result = numpy.ma.masked_values(values, eccodes.CODES_MISSING_DOUBLE)
        else:
            result = numpy.ma.masked_array(data=values)
        return result
    except eccodes.CodesInternalError:
        return None

def decode_bufr_array_attribute(msgid, key, attribute, required_length=None):
    '''returns a numpy masked array corresponding to the key and attribute.
    If the length is not the required_length then returns None'''
    key_to_call = key+'->'+attribute
    result = decode_bufr_array(msgid, key_to_call)
    if result is not None and required_length is not None:
        if len(result) != required_length:
            LOGGER.warning('%s has length %d instead of %d',
                           key_to_call, len(result), required_length)
            result = None
    return result

def get_time_periods(msgid, field_lists):
    """returns time periods of the current message as two lists of
    titles and indexs. Split out to make tools.get_time_periods_from_fields testable"""
    #working out time periods can be an expensive operation in big data sets so
    #  we only do that if we have to
    has_accumulated_fields = False
    for fieldname in field_lists['required']+field_lists['meteorology']:
        if field_is_accumulated(fieldname):
            has_accumulated_fields = True
            break
    if not has_accumulated_fields:
        return [], numpy.ma.asarray([])
    else:
        values = decode_bufr_array(msgid, 'timePeriod')
        if values is None:
            LOGGER.debug('no timePeriods')
            return [], numpy.ma.asarray([])
        units = decode_bufr_array_attribute(msgid, 'timePeriod', 'units', len(values))
        indexs = decode_bufr_array_attribute(msgid, 'timePeriod', 'index', len(values))
        if units is None or indexs is None:
            return [], numpy.ma.asarray([])
        else:
            return tools.get_time_periods_from_fields(values, units, indexs)

def field_is_accumulated(fieldname):
    '''Decides if this field is accumulated over a time period'''
    return '@' in fieldname

def process_message_get_field_array(msgid, fieldname, time_titles=None, time_indexs=None):
    '''If this is not an accumulated field then the dictionary will just be {field:values}
    None is returned is there is any problem getting that field, most probably because it
    is missing in the message.

    The values in the dictionary will always be numpy masked arrays.
    Accumulated fields could be specified as "fieldname@" in which case
      we return all accumualated values
      ie. a multiple key dictionary {field@acc1: values1, field@acc2:values2 ...}
    An alternative specification for an accumulated field is something like
      "fieldname@3h". In that instance only the 3hourly accumulation
      is put into the resulting dictionary'''
    if field_is_accumulated(fieldname):
        #this is an accumulated field so we have a few more hoops to go through
        if time_titles is None or time_indexs is None:
            raise IndexError, 'accumulated but time arrays None'
        field, required_title = fieldname.split('@')
        values = decode_bufr_array(msgid, field)
        if values is None:
            return None
        len_values = len(values)
        indexs = decode_bufr_array_attribute(msgid, field, 'index', len_values)
        if indexs is None or len(indexs) != len_values:
            LOGGER.debug('accumulated field: %s indexs length suspect', fieldname)
        result = {}
        for each in range(len_values):
            if isinstance(values.mask, numpy.ndarray) and values.mask[each]:
                value = None  #continue #this particular value is missing
            elif isinstance(values.mask, numpy.bool_) and values.mask:
                value = None#  continue #all of these values are missing
            else:
                value = values[each]

            #we want the precding time title = (value and unit)
            # so we work out the index in the sorted time_index array
            match_index = numpy.searchsorted(time_indexs,
                                             indexs[min(each, len(indexs)-1)],
                                             side='left') - 1
            if 0 <= match_index < len(time_titles):
                #If required, we step back to the most recent valid time_title
                if value is not None:
                    while match_index > 0 and time_titles[match_index] == '?':
                        match_index -= 1
                if time_titles[match_index] != '?' and \
                   (not required_title or required_title == time_titles[match_index]):
                    key = field + '@' + time_titles[match_index]
                    if not key in result:
                        result[key] = [value]
                    else:
                        result[key].append(value)
        for key, values in result.items():
            result[key] = numpy.ma.masked_equal(numpy.asarray(values), None)
    else:
        values = decode_bufr_array(msgid, fieldname)
        if values is None:
            return None
        else:
            result = {fieldname:values}
    return result

def process_message(msgid, field_lists):
    '''process one message into an array of reports'''
    data = {}
    report_count = eccodes.codes_get(msgid, "numberOfSubsets")
    time_titles, time_indexs = get_time_periods(msgid, field_lists)
    for fieldname in field_lists['required']:
        field_dict = process_message_get_field_array(msgid, fieldname,
                                                     time_titles, time_indexs)
        if field_dict is None:
            LOGGER.warning('missing required field: "%s"', fieldname)
            return {}, 0
        else:
            data.update(field_dict)

    missing_optional_fields = []
    for fieldname in field_lists['meteorology']:
        field_dict = process_message_get_field_array(msgid, fieldname,
                                                     time_titles, time_indexs)
        if field_dict is None:
            missing_optional_fields.append(fieldname)
        else:
            data.update(field_dict)

    if missing_optional_fields:
        LOGGER.info('missing optional fields: %s', missing_optional_fields)

    #for some report_types the report_count is NOT the number of subsets,
    #  so we have to deduce report_count from the length of a (required) field
    if "report_count_field" in field_lists:
        report_count_field = field_lists["report_count_field"]
        if report_count_field in data:
            report_count = len(data[report_count_field])

    #this is where we see if we can "locate" any unknown stations
    data = add_station_details(data)

    return data, report_count

def add_station_details(data):
    """see if we can "find" any unknown stations and thereby fill
    out missing lat, long, elevation. The lat/longs are needed for bounding_box
    calculations so we have to do this fairly early in processing"""
    is_wmo_id = 'blockNumber' in data and 'stationNumber' in data
    is_icao_id = 'CCCC' in data
    if not is_wmo_id and not is_icao_id or 'latitudeDisplacement' in data:
        #our station information can't help with these reports
        return data

    #since it isn't efficient to append to numpy arrays, we'll use lists for this bit
    lats, lons, elevations = [], [], []
    if 'latitude' in data:
        lats = data['latitude'].tolist()
        lons = data['longitude'].tolist() #assume longitude is there as well as latitude
    for heightkey in ['elevation', 'heightOfStation', 'heightOfStationGroundAboveMeanSeaLevel']:
        if heightkey in data:
            elevations = (data[heightkey]+0.0).tolist() #convert to floats
            break
    if is_wmo_id:
        length = len(data['blockNumber'])
    else:
        length = len(data['CCCC'])

    for _ in range(len(lats), length):
        lats.append(None)
        lons.append(None) #assume lons is the same length as lats
    for _ in range(len(elevations), length):
        elevations.append(None)

    found_a_station = False
    for each in range(length):
        if lats[each] is None or elevations[each] is None:
            if is_wmo_id:
                obsid = '%2.2d%3.3d' %(data['blockNumber'][each],
                                       data['stationNumber'][each])
            else:
                obsid = obsid = data['CCCC'][each]
            details = station_details.get_station(obsid)
            if details is not None:
                if lats[each] is None:
                    lats[each] = details[station_details.LATITUDE]
                    lons[each] = details[station_details.LONGITUDE]
                if elevations[each] is None and details[station_details.ELEVATION] is not None:
                    elevations[each] = float(details[station_details.ELEVATION])
                found_a_station = True

    if found_a_station: #convert lists back to masked arrays
        data['latitude'] = numpy.ma.masked_equal(lats, None)
        data['longitude'] = numpy.ma.masked_equal(lons, None)
        data['elevation'] = numpy.ma.masked_equal(elevations, None)

    return data

def translate_metar_data(data, timestamp):
    '''Produces a dictionary of numpy masked arrays which is compatible with the
    output of eccodes BUFR decoding. In the process it converts strings to
      floats or integers. Year and month are added from the timestamp.
    Converts everything to SI'''
    result = {}
    def _simple(key, values):
        return key, values.compressed().tolist()
    def _str_to_int(key, values):
        return key, [int(val) for val in values.compressed().tolist()]
    def _str_to_int_if_integer(key, values):
        return key, [int(val) if str(val).isdigit() else val \
                     for val in values.compressed().tolist()]
    def _temperature(key, values):
        return 'airTemperature', [tools.change_to_si_units('degC', val) \
                                 for val in values.compressed().tolist()]
    def _dewpoint(key, values):
        return key, [tools.change_to_si_units('degC', val) \
                     for val in values.compressed().tolist()]
    def _qnh_in_hpa(key, values):
        return 'pressureReducedToMeanSeaLevel', \
             [tools.change_to_si_units('hPa', float(val.replace('Q', ''))) \
              for val in values.compressed().tolist()]
    def _qnh_in_inhg00(key, values):
        return 'pressureReducedToMeanSeaLevel', \
             [tools.change_to_si_units('inhg00', float(val.replace('A', ''))) \
              for val in values.compressed().tolist()]
    def _windspeed(key, values):
        if 'windUnits' in data and \
                   len(data['windUnits'].compressed()) > 0:
            windunits = data['windUnits'].compressed()[0]
        else:
            windunits = 'knots'
        return key, [tools.change_to_si_units(windunits, int(val)) \
                     for val in values.compressed().tolist()]
    trans = {'latitude':_simple, 'longitude':_simple, 'CCCC':_simple,
             'name':_simple, 'elevation':_simple,
             'day':_str_to_int, 'hour':_str_to_int, 'minute':_str_to_int,
             'windDirection':_str_to_int_if_integer,
             'temperature':_temperature,
             'dewPointTemperature':_dewpoint,
             'qnhInHectoPascal':_qnh_in_hpa,
             'qnhInHundrethsOfInchOfMercury': _qnh_in_inhg00,
             'windSpeed':_windspeed}
    for key in trans:
        if key in data:
            if len(data[key].compressed()) > 0: # there are valid values
                try:
                    newkey, lst = trans[key](key, data[key])
                    result[newkey] = numpy.ma.asarray(lst)
                except ValueError:
                    LOGGER.warning('wrong type translating metar %s - ignoring that field', key)

    #ALWAYs get the year and month from timestamp since eccodes makes these 'undefined'
    result['year'] = numpy.ma.asarray([int(timestamp[:4])])
    result['month'] = numpy.ma.asarray([int(timestamp[4:6])])
    return result

def process_metar_message(msgid, field_lists, timestamp):
    """process current metar message"""
    for fieldname in field_lists['required'] + field_lists['meteorology']:
        if field_is_accumulated(fieldname):
            LOGGER.error('no support for accumulated metar fields like %s', fieldname)
            return {}, 0
    data = {}
    for fieldname in field_lists['required']:
        field_dict = process_message_get_field_array(msgid, fieldname)
        if field_dict is None:
            LOGGER.warning('no required field: "%s"', fieldname)
            return {}, 0
        else:
            data.update(field_dict)
    for fieldname in field_lists['meteorology']:
        field_dict = process_message_get_field_array(msgid, fieldname)
        if field_dict is not None:
            data.update(field_dict)
    result = translate_metar_data(data, timestamp)
    return result, 1

def decode_metar_file(filename, field_lists):
    '''Decoded metar reports are made to look like decoded BUFR reports'''
    result = []
    message_count = 0
    _, timestamp, _, _ = tools.parse_filename(filename)
    with open(filename) as metar_file:
        # loop through the messages in the file
        while True:
            msgid = eccodes.codes_metar_new_from_file(metar_file)
            if msgid is None:
                return result # decoded all of the messages in the file
            try:
                message_data, message_report_count = process_metar_message(msgid,
                                                                           field_lists,
                                                                           timestamp)
                if message_report_count:
                    message_data = add_station_details(message_data)
                    result.append((message_data, message_report_count))
                if message_report_count == 1:
                    LOGGER.info('found 1 metar for %s in message[%d]',
                                message_data['CCCC'][0], message_count)
                elif message_report_count == 0:
                    LOGGER.info('found no metar in message[%d]',
                                message_count)
                else:
                    LOGGER.info('found %s metars in message[%d]',
                                message_report_count, message_count)
                message_count += 1
            finally:
                # release the handle for this message
                eccodes.codes_release(msgid)

    return result

def decode_temp_report(data, report_count):
    '''decode lat,lon,time displacements into geniune latitude and longitude and
    datetime arrays.  We have to do this here instead of obs2aws.py
    because bounding_box calculations rely on latitude and longitude'''
    result = {}
    if 'latitudeDisplacement' in data and \
          len(data['latitudeDisplacement'].compressed()) >= report_count:
        lats, lons, dates = [], [], []
        basetime = datetime.datetime(data['year'][0],
                                     data['month'][0],
                                     data['day'][0],
                                     data['hour'][0],
                                     data['minute'][0],
                                     data['second'][0])
        lastdate = basetime
        baselat = data['latitude'].compressed()[0]
        baselon = data['longitude'].compressed()[0]
        for each in range(report_count):
            if not data['timePeriod'].mask[each]:
                lastdate = basetime + datetime.timedelta(days=0, seconds=data['timePeriod'][each])
            dates.append(lastdate.strftime('%Y%m%d%H%M%S'))
            if data['latitudeDisplacement'].mask[each]:
                lats.append(eccodes.CODES_MISSING_DOUBLE)
            else:
                lats.append(baselat + data['latitudeDisplacement'][each])
            if data['longitudeDisplacement'].mask[each]:
                lons.append(eccodes.CODES_MISSING_DOUBLE)
            else:
                lons.append(tools.lon_180_180(baselon + data['longitudeDisplacement'][each]))

        result['latitude'] = numpy.ma.masked_values(lats, eccodes.CODES_MISSING_DOUBLE)
        result['longitude'] = numpy.ma.masked_values(lons, eccodes.CODES_MISSING_DOUBLE)
        result['datetime'] = numpy.ma.asarray(dates)
        for field in data:
            if field not in ['latitude', 'longitude',
                             'latitudeDisplacement', 'longitudeDisplacement',
                             'datetime', 'year', 'month', 'day', 'hour', 'minute', 'second',
                             'timePeriod']:
                result[field] = data[field]
        return result
    else:
        return data

def decode_file(filename, report_type, field_lists):
    '''Returns a list of tuples, where each tuple is (message_data, report_count).
    message_data is dictionary of name: values
    where the name is the field and the values is a masked numpy array for all reports.
    Note the length of these arrays is not necessarily the same as the number of reports.
    Accumulated fields will have names like rain@60min or maxtemp@6h'''
    if not os.path.exists(filename):
        LOGGER.error('decode_file could not find file: %s', filename)
        return []
    if report_type == 'metar':
        # metar is so specialised that it has it's own decoding methods
        return decode_metar_file(filename, field_lists)

    # open bufr file
    result = []
    with open(filename) as bufr_file:
        # loop through the messages in the file
        for each_message in range(eccodes.codes_count_in_file(bufr_file)):
            # get handle for message
            msgid = eccodes.codes_bufr_new_from_file(bufr_file)
            if msgid is None:
                break # decoded all of the messages in the file
            try:
                # we need to instruct ecCodes to expand all the BUFR descriptors
                try:
                    eccodes.codes_set(msgid, 'unpack', 1)
                except eccodes.CodesInternalError:
                    LOGGER.warning('failed to unpack message in %s', filename)
                    break

                message_data, message_report_count = process_message(msgid, field_lists)
                if message_report_count > 0:
                    # special temp decoding, sorry...
                    if report_type == 'temp':
                        message_data = decode_temp_report(message_data, message_report_count)
                    result.append((message_data, message_report_count))
                LOGGER.debug('found %i reports in message[%d] of %s',
                             message_report_count, each_message, os.path.basename(filename))
            finally:
                # release the handle for this message
                eccodes.codes_release(msgid)

        return result
