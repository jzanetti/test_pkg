'''dynamodb rules'''
import copy
from datetime import datetime
import logging
import traceback

from botocore.exceptions import ClientError

from obs2aws import dynamo_db


LOGGER = logging.getLogger()



def datetime_str2datetime(datetime_str):
    """returns a datetime instance from a string YYYYMMDDHHMMSS"""
    return datetime.strptime(datetime_str, '%Y%m%d%H%M%S')

class DynDBWriter(object):
    """base class for writing to dynamodb"""
    def __init__(self, status, region, hostlocation, role_name):
        self.status = status
        self.region = region
        self.hostlocation = hostlocation
        self.role_name = role_name
        self.table_name = dynamo_db.get_table_name(status=self.status,
                                                   region=self.region, archive=False)
        self.client = self._get_client()
        if not self._table_exists(self.table_name):
            self._create_table(self.table_name)

    def _get_client(self):
        return dynamo_db.get_client_retry(self.status, self.region,
                                          self.hostlocation, self.role_name)

    def _table_exists(self, table_name):
        try:
            self.client.describe_table(TableName=table_name)
            return True
        except ClientError:
            return False

    def _create_table(self, table_name):
        dynamo_db.create_table(table_name, self.client)

    def write(self, data_dict):
        '''writes a row to dynamodb'''

        try:
            self.client.put_item(TableName=self.table_name, Item=data_dict)
#            LOGGER.debug('Wrote %s to %s', data_dict, self.__class__.__name__)
        except ClientError, err:
            LOGGER.warning('Failed to write item: %s to %s', data_dict,
                           self.__class__.__name__)
            LOGGER.warning(traceback.format_exc())
            if err.response['Error']['Code'] == "ExpiredTokenException":
                self.client = self._get_client()
                self.write(data_dict)
        except UnicodeDecodeError:
            LOGGER.warning('could not decode unicode: %s', data_dict)


class DynDBProdWriter(DynDBWriter):
    '''production writer to dynamodb'''

    def __init__(self, status, region, hostlocation, role_name):
        """ regions is a list of region name.
        """
        super(DynDBProdWriter, self).__init__(status, region, hostlocation, role_name)

    def _create_table(self, table_name):
        """ Override so we can enable time_to_live on the newly created table"""
        super(DynDBProdWriter, self)._create_table(table_name)
        dynamo_db.enable_time_to_live(table_name, self.client, True)

class DynDBArchiveWriter(DynDBWriter):
    """ Subclass DynDBWriter because we need to override the write method, as the table to
    write to depends on the date of the record
    """

    def __init__(self, status, region, hostlocation, role_name):
        self.region = region
        self.status = status
        self.hostlocation = hostlocation
        self.role_name = role_name
        self.client = self._get_client()
        self.existing_table_names = []

    def write(self, data_dict):

        date = datetime_str2datetime(data_dict['datetime']['S'])

        #make a copy, since we want to add the availability_time field and we
        #want to limit the affect of this change

        data_dict = copy.copy(data_dict)
        data_dict['availability_datetime'] = {
            'S':datetime.utcnow().strftime('%Y%m%d%H%M')}

        def write_helper():
            '''helps write to ddb'''
            desired_table_names = dynamo_db.get_archive_tables(date,
                                                               region=self.region,
                                                               status=self.status)
#            LOGGER.debug('DynDBArchiveWriter writing "%s"', str(data_dict))
            for table_name in desired_table_names:
                if not table_name in self.existing_table_names:
                    if not self._table_exists(table_name):
                        self._create_table(table_name)
                    self.existing_table_names.append(table_name)
                self.client.put_item(TableName=table_name, Item=data_dict)

        try:
            write_helper()
        except ClientError as err:
            if err.response['Error']['Code'] == "ExpiredTokenException":
                self.client = self._get_client()
                write_helper()
        except Exception as err:
            LOGGER.error('DynDBArchiveWriter writing Exception - %s', err.message)
            raise err
