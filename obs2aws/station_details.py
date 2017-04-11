""" Station details from several sources around the world.
    Checks if these source files have changed every hour.  If they have changed
    then the station information is reloaded"""
import csv
import datetime
import logging
import os

import MySQLdb

LF = '\n'

LATITUDE = 'latitude'
LONGITUDE = 'longitude'
ELEVATION = 'elevation'

LOGGER = logging.getLogger()

def _int_from_string(astring, default=None):
    try:
        return int(astring)
    except ValueError:
        return default

def parselatlon_statontable(astring, positive_char, negative_char):
    """ parses a string for a latitude or longitude"""
    lstr = astring.strip()
    if not lstr:
        return None
    lastch = lstr[-1]
    if not lastch in [positive_char, negative_char]:
        return None
    try:
        result = float(lstr[:-1])
    except ValueError:
        return None
    if lastch == negative_char:
        result = -result
    return result

def parselatlon_noaa(astring, positive_char, negative_char):
    """ parses a lat or lon from a NOAA string"""
    if not astring:
        return None
    lastch = astring[-1]
    if not lastch in [positive_char, negative_char]:
        return None
    result = 0.0
    factor = 1.0
    for value in astring[:-1].split('-'):
        result += int(value) * factor
        factor /= 60.0
    if lastch == negative_char:
        result = -result
    return result

def parse_bestresolution_latlon(hires, lores, positive_char, negative_char):
    """tries to parse hires and then lores string for lat/lon"""
    result = parselatlon_statontable(hires, positive_char, negative_char)
    if result is None:
        result = parselatlon_statontable(lores, positive_char, negative_char)
    return result

def read_table_station_from_line(line):
    """decodes a parts of a line from station.table fileto get latitude,
    longitude and height"""
    if len(line) < 242:
        return None, None
    identifier = line[:10].strip().upper()
    if not identifier:
        return None, None
    latitude = parse_bestresolution_latlon(line[209:220], line[32:39], 'N', 'S')
    if latitude is None:
        return None, None
    longitude = parse_bestresolution_latlon(line[221:242], line[40:49], 'E', 'W')
    if not longitude:
        return None, None
    height = _int_from_string(line[77:82].strip())
    return str(identifier), {LATITUDE:latitude, LONGITUDE:longitude, ELEVATION:height}

def noaa_station_data_from_row(row):
    """decodes a parts of a row from the NOAA station file where row[7] is latitude of
    the form dd-mm[N,S] or dd-mm-ss[N,S] and row[8] is longitude of form
    dd-mm[E,W] or dd-mm-ss[E,W] and height in row[11] is an optional integer"""
    if len(row) < 12:
        return None
    latitude = parselatlon_noaa(row[7], 'N', 'S')
    if latitude is None:
        return None
    longitude = parselatlon_noaa(row[8], 'E', 'W')
    if longitude is None:
        return None

    height = _int_from_string(row[11])
    return {LATITUDE:latitude, LONGITUDE:longitude, ELEVATION:height}

def is_modified(filename, file_mod_time):
    """True if has been modified """
    return os.path.exists(filename) and file_mod_time != os.path.getmtime(filename)

class StationDetails(object):
    """Details about a station """
    def __init__(self):
        """constructor"""
        self.station_table_filename = ''
        self.noaa_filename = ''
        self.known_stations = {}
        self.cursor = None
        self.noaa_file_age = None
        self.station_file_age = None
        self.last_reload_check_time = None
        self.enabled = False

    def is_loaded(self):
        """true if any station are known at all"""
        return self.known_stations != {}

    def reload(self):
        """Clears memory cache and reloads from files on disk"""
        self.known_stations = {}
        self.read_noaa_stations()
        self.read_table_stations()
        self.last_reload_check_time = datetime.datetime.utcnow()
        LOGGER.info('Have %s known stations', len(self.known_stations.keys()))

#    def reload_if_changed_files(self):
#        """Checks file creation times and reloads if needed"""
#        result = is_modified(self.station_table_filename, self.station_file_age)
#        if not result:
#            result = is_modified(self.noaa_filename, self.noaa_file_age)
#        if result:
#            self.reload()
#        return result

    def read_table_stations(self):
        """ reads text file station.table stations into a dictionary """
        if not os.path.exists(self.station_table_filename):
            LOGGER.warning('could not find station.table file "%s"', self.station_table_filename)
            return self.known_stations
        count = 0
        with open(self.station_table_filename, 'r') as textfile:
            lines = textfile.read().split(LF)
        for line in lines:
            station_id, data = read_table_station_from_line(line)
            if station_id is not None:
                self.known_stations[station_id] = data
                count += 1
        self.station_file_age = os.path.getmtime(self.station_table_filename)
        LOGGER.info(' Loaded %i station records from "%s"', count, self.station_table_filename)
        return self.known_stations


    def read_noaa_stations(self):
        """ reads text file of NOAA stations into a dictionary """
    #    wget -c http://weather.noaa.gov/data/nsd_bbsss.txt
    #72;656;KSFD;Winner, Bob Wiley Field Airport;SD;United States;4;43-23-26N;099-50-33W;;;619;;
    #93;246;NZRO;Rotorua Aerodrome;;New Zealand;5;38-07S;176-19E;38-07S;176-19E;285;294;
    #block;synop;icao;name;?;country;??;lat;lon;lat2;lon2;height;?;
    #0      1     2    3   4  5      6  7   8   9    10   11     12
        if not os.path.exists(self.noaa_filename):
            LOGGER.warning('could not find noaa file "%s"', self.noaa_filename)
            return self.known_stations
        count = 0
        with open(self.noaa_filename, 'r') as csvfile:
            stationreader = csv.reader(csvfile, delimiter=';')
            for row in stationreader:
                station_id = '{}{}'.format(row[0], row[1])
                station_id_icao = row[2].strip().upper()
                data = noaa_station_data_from_row(row)
                if data is not None:
                    count += 1
                    self.known_stations[station_id] = data
                    if len(station_id_icao) == 4 and station_id_icao.isalpha():
                        self.known_stations[station_id_icao] = data
        self.noaa_file_age = os.path.getmtime(self.noaa_filename)
        LOGGER.info(' Loaded %i noaa station records from "%s"', count, self.noaa_filename)
        return self.known_stations

    def prism_station_details(self, station_id):
        """get details of prism station """
        if self.cursor is None: #singleton pattern
            db_conn = MySQLdb.connect(host="kp-amps-director1",
                                      user="amps", passwd="amps", db="AMPS")
            self.cursor = db_conn.cursor()
        if station_id.isdigit() and int(station_id[0]) in range(0, 10):
            selectstatement = 'SELECT latitude,longitude,elevation ' \
                'FROM StationLocation WHERE stationId = "%s"' % (station_id)
        else:
            selectstatement = \
                'SELECT latitude,longitude,elevation ' \
                'FROM StationLocation, Station ' \
                'WHERE StationLocation.stationId = Station.stationId and ' \
                'Station.icaoId =  "%s"' % (station_id[1:])
        try:
            self.cursor.execute(selectstatement)
            first_result = self.cursor.fetchone()
        except Exception: #what sorts should this be?
            return None
        if not first_result:
            return None
        try:
            result = {LATITUDE:first_result[0],
                      LONGITUDE:first_result[1],
                      ELEVATION:first_result[2]}
            return result
        except Exception: #what types of error should this be?
            return None

    def set_filenames(self, directories):
        """set the filenames and reload if necessary """
        station_table_filename = self.station_table_filename
        noaa_filename = self.noaa_filename
        if directories is None:
            self.enabled = False
            self.known_stations = {}
            self.station_table_filename = ''
            self.noaa_filename = ''
        else:
            if not isinstance(directories, list):
                directories = [directories]
            self.enabled = True
            for directory in directories:
                station_table_path = os.path.join(directory, 'station.table')
                noaa_path = os.path.join(directory, 'nsd_bbsss.txt')
                if os.path.exists(station_table_path) or \
                   os.path.exists(noaa_path):
                    station_table_filename = station_table_path
                    noaa_filename = noaa_path
                    break

            if station_table_filename != self.station_table_filename or \
                        noaa_filename != self.noaa_filename:
                self.station_table_filename = station_table_filename
                self.noaa_filename = noaa_filename
                self.reload()

    def station_details_for(self, station_id):
        """Get lat long and height for a station.
        Returns a dictionary {'latitude':lat, 'longitude':lon, 'elevation':height}
        or None if not found"""
        if not self.enabled:
            return None
        station_id_upper = station_id.upper()
        if not self.is_loaded():
            self.reload()
        if not station_id_upper in self.known_stations:
            prism_station = self.prism_station_details(station_id_upper)
            self.known_stations[station_id_upper] = prism_station
        return self.known_stations[station_id_upper]

STATIONS = StationDetails() # the singleton global object

def set_stations_filenames(directories):
    """a wrapper method to load station information into the global object.
    Call this at startup and probably at regular intervals of about a day.
    If you want to make STATIONS disabled (so there are no known stations)
    and can never be any, then pass directories as None"""
    STATIONS.set_filenames(directories)

def get_station(station_id):
    """Get lat long and height for a station.
    This is the method you should call in general processing.
    set_stations_filenames should probably be called prior to this.
    Returns Returns a dictionary {'latitude':lat, 'longitude':lon, 'elevation':height}
    or None if the station is not found"""
    return STATIONS.station_details_for(station_id)
