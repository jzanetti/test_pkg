"""
This creates the correct objects for upload to dynamodb

Created on Thu Jan 19 14:41:19 2017

@author: Cory
"""
#standard python library
import copy
import datetime
import glob
import logging
import numpy
import os
import traceback

#third party
import Geohash
import simplejson

#this package
from . import tools
from . import decode
from . import upload_to_s3
from . import dynamo_db


LOGGER = logging.getLogger()



def decode_file_and_populate_writing_queues(filename, field_lists, report_type,
                                            s3_base_dict, ddb_queues,
                                            csv_queue, s3queues, failed,
                                            ddb_recent_lifespan='3d'):
    """decodes a raw observation file and populates queues for ddb, s3,
    and csv writing.
    Files which fail to decode will be appended to the failed list.
    ddb_queues and s3queus are dictionaries which MUST have the same keys"""
    try:
        _, timestamp, gtsheader, file_ext = tools.parse_filename(filename)
        bounding_box, times, ddb_rows = get_s3keyinfo_ddb_rows(filename,
                                                               field_lists,
                                                               report_type,
                                                               timestamp)
        if bounding_box and times:
            #there is only one ascat csv destination
            if report_type == 'ascat':
                for ddb_row in ddb_rows:
                    csv_queue.put((filename, ddb_row))

            #work out the expiry date for DDB recent items
            secs = tools.parse_duration_to_seconds(ddb_recent_lifespan)
            if secs is not None:
                #ttl must be in UTC of course
                ttl_datetime = datetime.datetime.utcnow() + datetime.timedelta(seconds=secs)
                ttl = (ttl_datetime-datetime.datetime(1970, 1, 1)).total_seconds()
                LOGGER.debug('setting ttl to %s (%s)', ttl_datetime, ttl)
            else:
                ttl = None

            #potentially both recent and archive s3 and DDB destinations
            for key, s3_base in s3_base_dict.iteritems():
                s3key = upload_to_s3.get_s3key(s3_base, report_type, gtsheader,
                                               file_ext, bounding_box, times)
                if s3key.startswith('s3://'):
                    s3queues[key].put((filename, s3key));
                
                indexi = 0;
                for ddb_row in ddb_rows:
                    ddb_row_copy = copy.copy(ddb_row)
                    ddb_row_copy.set_attribute('s3key', s3key) #copes with blank
                    
                    if key == "recent":
                        ddb_row_copy.set_attribute('ttl', ttl) #copes with None
                    ddb_queues[key].put((filename, ddb_row_copy));
                    #if key == "recent":
                    #    ddb_row_copy.set_attribute('ttl', ttl) #copes with None
                        #indexi = indexi + 1;
                        #print indexi
                    
                    #print key + ': ' + str(ddb_queues[key].get(False));
                    
                    #if key == "recent":
                    #    ddb_row_copy.set_attribute('ttl', ttl) #copes with None

    except:
        LOGGER.error("failed to decode file %s", filename)
        LOGGER.error(traceback.format_exc())
        failed.append(filename)
        return

def find_files_to_process(patterns):
    '''finds all of the observation files. Files must have a basename
    which has at least one underscore in it and a timestamp is the second element
    in that basename.'''
    result = []
    for pattern in patterns:
        for new_file in glob.glob(pattern):
            if '_' in os.path.basename(new_file):
                result.append(new_file)

    result.sort(key=lambda x: os.path.basename(x).split('_')[1])
    return result


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

def make_links(src_obs_filenames, dest_work_dirs):
    """takes a list of source observation file paths, of the form 
    '.../<obs_type>/<filename.bufr>' and a list of destination directories
    (i.e. one for each region), and creates a '.../<obs_type>/<filename.bufr>' 
    hardlink in each destination work directory and then deletes the original
    file"""
    obs_type_dirs = []
    for f in src_obs_filenames:
        for dest_work_dir in dest_work_dirs:
            dest = os.path.join(dest_work_dir,
                                os.path.split(os.path.split(f)[-2])[1],
                                os.path.basename(f))
            obs_type_dir = os.path.dirname(dest)
            if not obs_type_dir in obs_type_dirs:
                obs_type_dirs.append(obs_type_dir)
                if not os.path.exists(obs_type_dir):
                    LOGGER.debug('making directory(s) %s', obs_type_dir)
                    os.makedirs(obs_type_dir)
                
            LOGGER.debug('making hardlink at %s to %s', dest, f)
            try:
                os.link(f, dest)
            except OSError:
                LOGGER.warning("failed to make hardlink at %s to %s", dest, f)
        os.remove(f)

def read_config(config_file=None):
    '''read the specified configuration file and return a dictionary - not in unicode!'''
    if config_file is None:
        config_file = os.path.join(os.environ["CONDA_PREFIX"],
                                   'etc/obs2aws/decode_and_upload_obs.json')
    with open(config_file, 'r') as cfg:
        config = tools.byteify(simplejson.load(cfg))
    LOGGER.info('loaded configuration from %s', config_file)
    return config

def s3_thread(in_queue, role_name, keys_path, s3_options, failed):
    '''thread to upload to s3'''
    while True:
        filename, s3key = in_queue.get()
        try:
            upload_to_s3.s3copy(filename, s3key, role_name, keys_path, s3_options)
        except:
            LOGGER.error("Failed to upload %s to %s", filename, s3key)
            LOGGER.error(traceback.format_exc())
            failed.append(filename)
        in_queue.task_done()

def writer_thread(writer, in_queue, failed):
    '''thread to write to either dynamodb or csv'''
    while True:
        item = in_queue.get()
        if item == 'QUIT':
            in_queue.task_done()
            return
        filename, line = item
        try:
            writer.write(line)
        except:
            LOGGER.error("Failed to write line from %s to %s. line: %s",
                         filename, writer.__class__.__name__, line)
            LOGGER.error(traceback.format_exc())
            failed.append(filename)
        in_queue.task_done()
