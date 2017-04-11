import os
import logging

from pycast.distributor import add_to_distribution_queue

LOGGER = logging.getLogger()


class DummyCsvWriter(object):
    """A class for csv which does nothing"""
    def __init__(self, filename, distribute=False, rm_none=True):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def write(self, ddb_row):
        """stubbed method for write"""
        pass


class AscatCsvWriter(object):

    """ Implement a csv file writer class that work on ScatterometerFileDecoder
    instances.
    """
    FIELDS_OUT = ['time',
                  'latitude',
                  'longitude',
                  'direction',
                  'speed(knots)']

    def __init__(self, filename, distribute=False, rm_none=True):
        """ """
        self.filename = filename
        self.distribute = distribute
        self.rm_none = rm_none
        self.num_lines = 0

    def __enter__(self):
        """
        """
        LOGGER.info(
            "create file %s with columns %s", self.filename, self.FIELDS_OUT)
        self.csvfile = open(self.filename, 'w')
        self.csvfile.write(','.join(self.FIELDS_OUT) + "\n")
        return self

    def __exit__(self, type, value, traceback):
        """
        """
        self.csvfile.close()
        if not self.num_lines:
            LOGGER.info("All the records are None. DELETING the CSV file!")
            os.remove(self.filename)
        elif self.distribute:
            add_to_distribution_queue([self.filename, ])

    def write(self, ddb_row):
        """ Write data read from a scatterometer bufr file
        (as a dynamo_db.ObsDBRow instance) to a CSV file.
        Timestamp is to the minute, ie 12 characters long
        """
        # Remove line containing None

        if self.rm_none:
            if None in [ddb_row["windDirectionAt10M"], ddb_row["windSpeedAt10M"]]:
                return
        self.csvfile.write('{}, {:.3f}, {:.3f}, {:.2f}, {:.2f}\n'.format(
            ddb_row['datetime']['S'][:12], float(ddb_row['latitude']['N']),
            float(ddb_row['longitude']['N']), float(ddb_row["windDirectionAt10M"]['N']),
            float(ddb_row["windSpeedAt10M"]['N'])*1.94384))
        self.num_lines += 1
