from obs2aws import obs2aws
from obs2aws import decode
from obs2aws import upload_to_s3
from obs2aws import dynamo_db
import logging
import datetime
import Geohash

LOGGER = logging.getLogger()
def get_s3keyinfo_ddb_rows(filename, field_lists, report_type, timestamp):
    """returns the ingredients for the prospective s3 key and list of
    DynamoDB entries from a raw observation file"""
    ddb_rows = []
    messages = decode.decode_file(filename, report_type, field_lists)
    LOGGER.debug('filename: %s #messages: %d', filename, len(messages))
    bounding_box = upload_to_s3.get_bounding_box(messages)
    LOGGER.debug('bounding box: %s', bounding_box)
    times = {}
    if bounding_box:
        times = upload_to_s3.get_nominal_times(messages, timestamp, datetime.datetime.utcnow())
        LOGGER.debug('times: %s', times)
        if times:
            each = 0
            for data, report_count in messages:
                rows = make_dyndb_rows(data, report_count, report_type)
                ddb_rows.extend(rows)
                LOGGER.info('made %d rows from message[%d] in %s', len(rows), each, filename)
                each += 1
    return bounding_box, times, ddb_rows


def make_dyndb_rows(data, report_count, report_type):
    '''makes the rows for dynamodb upload'''
    ###THIS COULD BE MADE MUCH FASTER WITH NUMPY VECTORISATION, PARTICULARLY FOR ASCAT
    #by_length is a dictionary where each key is a number and the corresponding value
    #   values is the list of fieldnames that have 'number' occurances in the file

    #start with empty lists of lengths
    by_length = {len(data[key]):[] for key in data}
    by_length[1] = []  # always include singleton length
    for name, values in data.items():
        by_length[len(values)].append(name)

    #iterate through data, writing to live and archive DynamoDB tables

    # check the lengths of these numpy masked arrays are sensible
    for actual_length in by_length:
        if actual_length > 1 and (actual_length % report_count != 0):
            if not report_type.startswith('amv'):
                LOGGER.info('Suspect length of %d in %s file with %i reports: ' \
                            ' fields: %s',
                            actual_length, report_type,
                            report_count, by_length[actual_length])

    rows = []
    #Go through the fields in the file
    for index in range(report_count):
        ddb_row = dynamo_db.ObsDBRow()

        # Simplest case: one value for each report
        for fieldname in by_length[report_count]:
            ddb_row.set_attribute(fieldname, data[fieldname][index])

        # Add all of the "singleton" fields that occur once in the file to this row
        if report_count > 1:
            for fieldname in by_length[1]:
                ddb_row.set_attribute(fieldname, data[fieldname][0])

        # special ascat code
        if report_type == 'ascat':
            if not 4*report_count in by_length:
                LOGGER.info('ascat data does not have 4*report_count(=%s) in by_length',
                            4*report_count)
                #no data for any of the selected wind vectors, give up
                return rows

            if index >= len(data["indexOfSelectedWindVector"]):
                #probably no scat data in the file at all ... but keep trying
                LOGGER.debug('ascat[%i] index >= len(indexOfSelectedWindVector):%i - ignoring',
                             index, len(data["indexOfSelectedWindVector"]))
                continue
            wi = data["indexOfSelectedWindVector"][index]
            if not 1 <= wi <= 4:
                # no windvector selected for this WVC
                LOGGER.debug('invalid selected index of %i for ascat[%i] - ignoring', wi, index)
                continue

            #only include data from selected windfactor
            for fieldname in by_length[4*report_count]:
                value = data[fieldname][report_count * (wi-1) + index]
                ddb_row.set_attribute(fieldname, value)

        else: # for anything left over
            for length in by_length:
                if length not in [1, report_count]:
                    for fieldname in by_length[length]:
                        fieldsize = length / report_count
                        if fieldsize > 0:  # there are at least report_count values
                            validvalues = data[fieldname][index:index+fieldsize].compressed()
                            if len(validvalues) > 0:
                                ddb_row.set_attribute(fieldname, validvalues[0])

                        #for old report types we try to use the index(th) element
                        elif index < length and report_type in ["metar", "synop", "ship"]:
                            ddb_row.set_attribute(fieldname, data[fieldname][index])

                        # who really knows?  We'll use the first valid value
                        else:
                            ddb_row.set_attribute(fieldname, data[fieldname][0])
                            if fieldname not in by_length[1]:
                                by_length[1].append(fieldname)


        #We must set the station identifier, lat,lon, (evelation) and time.
        if ddb_row.set_station_id(report_type, index) and \
           ddb_row.has_station_location() and \
           ddb_row.set_report_time(index):

            #add geohash
            geo = Geohash.encode(float(ddb_row['latitude']['N']),
                                 float(ddb_row['longitude']['N']),
                                 2)
            ddb_row.set_attribute('report_type_geohash', '{}_{}'.format(
                report_type, geo))

            rows.append(ddb_row)
    return rows
