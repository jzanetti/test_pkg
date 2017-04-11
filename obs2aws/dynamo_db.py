'''classes and methods for dealaing with dyanmodb'''
import datetime
import logging
import time

import botocore
import numpy

from AMPSAws import utils, connections

from . import tools

LOGGER = logging.getLogger()
IDENTIFIER = 'obs_id'

class ObsDBRow(dict):
    '''a single report in dynamodb'''
    def set_attribute(self, fieldname, value):
        '''Only adds the value to the row if value is valid'''
        if value is not None and not isinstance(value, numpy.ma.core.MaskedConstant):
            if isinstance(value, float):
#                self[fieldname] = decimal.Decimal("%.6f" % value)
                self[fieldname] = {'N':"%.6f" % value}
            elif isinstance(value, int):
                self[fieldname] = {'N':str(value)}
            else:
                #DynamoDB will not tolerate empty strings, so we just dont
                #add the column. Also omit hex strings encoding missing
                if (value != '') and (value != len(value) * '\xff'):
                    self[fieldname] = {'S':str(value)}


    def set_report_time(self, index):
        '''set datetime attribute if that succeeds delete other time fields'''
        result = 'datetime' in self
        if not result:
            try:
                if 'second' in self:
                    seconds = int(self['second']['N'])
                else:
                    seconds = 0
                item_time = datetime.datetime(int(self['year']['N']),
                                              int(self['month']['N']),
                                              int(self['day']['N']),
                                              int(self['hour']['N']),
                                              int(self['minute']['N']),
                                              seconds)
                self.set_attribute('datetime', item_time.strftime('%Y%m%d%H%M%S'))
                result = True
            except ValueError:
                LOGGER.warning('bad time value in row[%s]', index)
            except KeyError:
                LOGGER.warning('missing a time field in row[%s]', index)
        if result:
            #since we have datetime defined we don't need the individual time fields
            for key in ['year', 'month', 'day', 'hour', 'minute', 'second']:
                if key in self:
                    del self[key]
        return result

    def _set_station_attribute(self, oldkey, newkey, delete_key=True, suffix=None):
        result = oldkey in self
        if result:
            newvalue = self[oldkey]
            if suffix is not None:
                #then the newvalue must be a string
                newvalue = {'S':newvalue[newvalue.keys()[0]] + suffix}
            self[newkey] = newvalue
            if delete_key:
                del self[oldkey]
        return result


    def has_station_location(self):
        """Returns true if the latitide and longitude are filled in correctly"""
        result = 'latitude' in self and 'longitude' in self
        if result:
            result = (-90.0 <= float(self['latitude']['N']) <= 90.0) and \
                     (-180 <= float(self['longitude']['N']) < 180)
        return result

    def set_station_id(self, report_type, index):
        '''set the station identifier. Unfortunately this is specific to report_type'''
        if report_type in ['synop', 'temp', 'pilot', 'radarvvp']:
            result = 'blockNumber' in self and 'stationNumber' in self
            if result:
                block = int(self['blockNumber']['N'])
                number = int(self['stationNumber']['N'])
                if report_type in ['radarvvp', 'temp']:
                    self.set_attribute(IDENTIFIER, '%2.2d%3.3d_%s_%s' %(block,
                                                                        number,
                                                                        index,
                                                                        report_type))
                else:
                    self.set_attribute(IDENTIFIER, '%2.2d%3.3d_%s' %(block, number,
                                                                     report_type))
                del self['blockNumber']
                del self['stationNumber']
        elif report_type == 'metar':
            result = self._set_station_attribute('CCCC',
                                                 IDENTIFIER,
                                                 suffix='_'+report_type)
        elif (report_type is not None and report_type.startswith('amv')) or report_type == 'ascat':
            result = self._set_station_attribute('satelliteIdentifier',
                                                 IDENTIFIER,
                                                 suffix='_{}_{}'.format(index, report_type))
        elif report_type == 'ship':
            result = self._set_station_attribute('shipOrMobileLandStationIdentifier',
                                                 IDENTIFIER,
                                                 suffix='_'+report_type)
        elif report_type == 'buoy':
            result = self._set_station_attribute('marineObservingPlatformIdentifier',
                                                 IDENTIFIER,
                                                 suffix='_'+report_type)
        elif report_type == 'amdar':
            result = self._set_station_attribute('aircraftRegistrationNumberOrOtherIdentification',
                                                 IDENTIFIER,
                                                 suffix='_'+report_type)
        else:
            LOGGER.error('error unknown report_type "%s"', report_type)
            result = False

        return result


def get_client_retry(status, region, hostlocation, role_name):
    """ Indefinitely try to get an Amazon client. """
    #may need backoff algorithm??
    conv = tools.byteify(utils.get_conventions(status))
    keys_path = conv[status][hostlocation]['path_to_iam_keys']
    try:
        client = connections.get_client(
            'dynamodb', status=status, region_name=region,
            role_name=role_name, keys_path=keys_path)
    except:
        LOGGER.warning('Failed to get connection to dynamodb, retrying in 10s.')
        time.sleep(10)
        client = get_client_retry(status, region, hostlocation, role_name)

    return client


def get_table_name(status, region, archive=True):
    '''Name of the table.  For archives this contains the year'''
    conv = tools.byteify(utils.get_conventions(status))
    if archive:
        tbl_name = conv[status][region]["archived_observations_database_prefix"]
    else:
        tbl_name = conv[status][region]["recent_observations_database_name"]
    return tbl_name


def get_archive_tables(date, status, region, overlap=7):
    """ Archive data contains one year of data plus a two week overlap at each
        year end.

        Returns:
        -------
            A list iterator with the name(s) of the archive tables to query or write
            data in.
    """
    years = {d.year for d in [date, date + datetime.timedelta(overlap),
                              date - datetime.timedelta(overlap)]}
    table_archive = get_table_name(status, region, archive=True)

    return (table_archive.replace('<yyyy>', str(year)) for year in years)


def create_table(table_name, client):
    """create a dynamodb table using boto3"""
    LOGGER.debug('creating table: %s', table_name)
    table = client.create_table(
        AttributeDefinitions=[
            {'AttributeName': 'obs_id', 'AttributeType': 'S'},
            {'AttributeName': 'datetime', 'AttributeType': 'S'},
            {'AttributeName': 'report_type_geohash', 'AttributeType': 'S'}
        ],
        TableName=table_name,
        KeySchema=[{'AttributeName': 'obs_id', 'KeyType': 'HASH'},
                   {'AttributeName': 'datetime', 'KeyType': 'RANGE'}],
        ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 100},
        GlobalSecondaryIndexes=[{'IndexName':'RecordTypeGeohashIndex',
                                 'KeySchema':[{'AttributeName': 'report_type_geohash',
                                               'KeyType': 'HASH'},
                                              {'AttributeName': 'datetime',
                                               'KeyType': 'RANGE'}],
                                 'Projection': {
                                     'ProjectionType': 'ALL'
                                 },
                                 'ProvisionedThroughput':{'ReadCapacityUnits': 10,
                                                          'WriteCapacityUnits': 200}}])
    client.get_waiter('table_exists').wait(TableName=table_name)
    LOGGER.info('created table: %s=%s', table_name, str(table))
    return table

def enable_time_to_live(table_name, client, enabled=True, attribute='ttl'):
    """enable time to live on the specified table"""
    try:
        resp = client.update_time_to_live(TableName=table_name,
                                          TimeToLiveSpecification={'Enabled':enabled,
                                                                   'AttributeName':attribute})
        if resp and 'ResponseMetadata' in resp and 'HTTPStatusCode' in resp['ResponseMetadata']:
            LOGGER.info('set time_to_live on %s OK: %s', table_name, resp)
        else:    
            LOGGER.warning('problem setting time_to_live on %s: %s', table_name, resp)
    except botocore.exceptions.ClientError:
        LOGGER.error('Could not set time_to_live on %s to %s', table_name, enabled)
