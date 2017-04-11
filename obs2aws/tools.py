#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
This module defines common constants and methods which other parts of obs2aws use

Created on Thu Jan 12 11:51:05 2017

@author: wim
"""
import logging
import numpy
import os
import re
import pint

from pycast.eventMon import eventRec, uniqueId, newTimestamp

LOGGER = logging.getLogger()

def parse_filename(filename):
    '''pull apart the filename into report_type, timestamp, gtsHeader, and file_extension'''
    basename = os.path.basename(filename)
    parts = basename.split('_')
    try:
        report_type = parts[0]
        timestamp = parts[1]
        if len(timestamp) != 14:
            raise ValueError, 'parse_filename timestamp "{}" not length 14'.format(timestamp)
        remainder = '_'.join(parts[2:])
        if '.' in remainder:
            gtsheader = remainder.split('.')[0]
            file_ext = remainder.split('.')[-1] #what else should we do with multiple "."?
        else:
            gtsheader = remainder
            file_ext = '.'

        return report_type, timestamp, gtsheader, file_ext
    except IndexError as err:
        raise IndexError, 'parse_filename could not decode "{}" - {}'.format(basename, err)

def byteify(data):
    '''turns unicode into ascii'''
    if isinstance(data, dict):
        return {byteify(key): byteify(value)
                for key, value in data.iteritems()}
    elif isinstance(data, list):
        return [byteify(element) for element in data]
    elif isinstance(data, unicode):
        return data.encode('utf-8')
    else:
        return data

class PintRegistry(object):
    """encapsulates pint registry for unit conversions"""
    def __init__(self):
        self.path = ''
        self.registry = pint.UnitRegistry()
        self.warned = False
        self.unknown = {}

    def loaded(self):
        """returns True if the unit registry extension has been found"""
        return self.path != ""


    def load(self, paths):
        """only loads registry if not already done so"""
        if not self.loaded():
            for path in paths:
                if os.path.exists(path):
                    self.registry.load_definitions(path)
                    self.path = path
                    self.warned = False
                    self.unknown = {}
                    LOGGER.debug('loaded pint unit registry from %s', path)
                    break

UREG = PintRegistry()
UREG.load([os.path.join(os.environ["CONDA_PREFIX"], 'etc/obs2aws/unit_registry.txt')])



def change_to_si_units(old_unit, old_value, key='', is_lowercase=False):
    '''change the old value in old_units to si units.
    If old_value is not parsable to a number, then logs an error
      and returns the old_value
    If old_unit is not known then adds that unit to UREG dictionary of unknown units
      and returns None.
    If old_unit is already in SI then the old value is returned
    If old_unit is known and not SI then the converted value is returned.'''
    if not UREG.loaded():
        if not UREG.warned:
            LOGGER.warning('pint registry has not been loaded')
            UREG.warned = True
    if old_unit is None or old_value is None:
        return None
    unit = old_unit
    if not is_lowercase:
        unit = old_unit.lower()
    if unit in ['c', u'c', 'degc', u'degc', 'degC', 'C']:
        unit = 'degC' #a hack?
    try:
        if UREG.registry.parse_expression(unit).to_base_units().magnitude == 1: #already in SI
            return old_value
        try:
            old_value_asfloat = float(old_value)
        except ValueError:
            LOGGER.warning('"%s" is not a valid float for unit change', old_value)
            return old_value
        base_units = UREG.registry.Quantity(old_value_asfloat, unit).to_base_units()
        return base_units.magnitude
    except pint.UndefinedUnitError:
        if not is_lowercase:
            return change_to_si_units(unit.lower(), old_value, key, True)
        else:
            if not unit in UREG.unknown:
                error = 'unit "{}" for key: "{}" is not known'.format(unit, key)
                LOGGER.warning(error)
                UREG.unknown[unit] = error
            return None

def lon_180_180(lon):
    '''returns -180 < result <= 180'''
    if lon > 180:
        return lon - 360.0
    elif lon <= -180:
        return lon + 360.0
    return lon


def create_event(runinit, process, source, destination):
    """Create an event for AppSupport"""
    evrec = eventRec(uid=uniqueId(), timestamp=newTimestamp())
    evrec.setKeyValue('runinit', runinit)
    evrec.setKeyValue('process', '{}'.format(process))
    evrec.setKeyValue('system', 'obs2aws')
    evrec.setKeyValue('platform', 'Observation Processing')
    evrec.appendKeyValue('source', source)
    evrec.appendKeyValue('destination', destination)
#    LOGGER.debug('evrec: %s', evrec.to_string())
    return evrec


def run_with_monitor(fun, args, runinit, process, source, destination):
    """ Encapsulate a function to send event to the event Viewer.
    """
    event_monitor = create_event(runinit, process, source, destination)
    event_monitor.send('Start')
    LOGGER.debug('Sent Start event for "%s" to eventsViewer', process)

    try:
        fun(*args)
    except:
        event_monitor.send('Fail')
        raise

    event_monitor.send('End')

def parse_duration_to_seconds(time_str):
    """works out the duration in seconds from strings like "3d", "2h", "7m", "55s" or
    combinations such as "3d12h". Returns the number of seconds or None if parsing fails"""

    pattern = r'((?P<days>\d+?)d)?' \
               '((?P<hours>\d+?)h)?' \
               '((?P<minutes>\d+?)m)?' \
               '((?P<seconds>\d+?)s)?'
    parts = re.match(pattern, time_str)
    result = None
    if parts is not None:
        parts = parts.groupdict()
        multiplier = {'days': 60 * 60 * 24,
                      'hours': 60 * 60,
                      'minutes': 60,
                      'seconds': 1}
        #'years': 60 * 60 * 24 * 365,
        #'months': 60 * 60 * 24 * 30,
        #'weeks': 60 * 60 * 24 * 7,
        for (key, value) in parts.iteritems():
            if value is not None:
                increment = int(value) * multiplier[key]
                if result is None:
                    result = increment
                else:
                    result += increment
    return result

def get_time_periods_from_fields(values, units, indexs):
    '''Takes 3 lists of equal length and
    returns time periods of the current message as a list of tuples (title, index) '''
    info = []
    #If a numpy masked "array" has only one element, then the mask for that element 
    # is a single property of the array, so you read the mask without using an index
    # "values.mask"
    #If there is more than one element, the mask is a genuine numpy array of booleans.
    # and you read the mask using an index   "values.mask[index]"
    #each_masked lets us know which situation is which
    each_masked = isinstance(values.mask, numpy.ndarray)
    len_values = len(values)
    for each in range(len_values):
        value = values[each]
        try:
            index = indexs[each]
            unit = units[each]
            if each_masked and values.mask[each]:
                title = '?'
                value = None
            elif not each_masked and values.mask:
                title = '?'
                value = None
            else:
                title = str(abs(values[each]))+units[each]
        except (IndexError, ValueError, numpy.ma.MaskError):
            title = '?'
            LOGGER.info('problem with time[%i]', each)
        info.append({'t':title, 'v':value, 'i':index, 'u':unit})

    #If there are two consecutive time periods then we interpret those as 
    # valid_from : valid_to 
    for each in range(1, len_values):
        if (info[each]['v'] is not None and info[each-1]['v'] is not None) and \
           (info[each]['u'] == info[each-1]['u']) and \
           (info[each]['i'] == info[each-1]['i'] + 1):
            if info[each]['v'] == 0:
                info[each]['t'] = '?'#valid_to is nominal time, pretend it's missing
            else:
                info[each]['t'] = '{}-{}{}'.format(abs(info[each-1]['v']),
                                                   abs(info[each]['v']),
                                                   info[each]['u'])
                info[each-1]['t'] = info[each]['t']

    titles = [info[each]['t'] for each in range(len_values)]
    return titles, indexs

