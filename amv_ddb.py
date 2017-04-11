from collections import namedtuple


TRANS = namedtuple("TRANS", ("tuple_name", "bufr_name"))

def custum_namedtuple(year="year",
                      month="month",
                      day="day",
                      hour="hour",
                      minute="minute",
                      second="second",
                      latitude="latitude (high accuracy)",
                      longitude="longitude (high accuracy)", **kwargs):

    requested_fields = {"year": year,
                        "month": month,
                        "day": day,
                        "hour": hour,
                        "minute": minute,
                        "second": second,
                        "latitude": latitude,
                        "longitude": longitude}
    requested_fields.update(kwargs)

    requested_fields = [TRANS(k, v) for k, v in requested_fields.items()]
    BUFR_FIELDS = namedtuple("BUFR_FIELDS", [x.tuple_name for x in requested_fields]);
    print BUFR_FIELDS;

    class BufrFields(BUFR_FIELDS):

        _translate = requested_fields

        def get_datetime(self):
            """return a basetime for the record specified by the index
            argument"""
            if self.year[0] < 100:  # will certainly retire before 1 january 2100
                self.year[0] += 2000

            return datetime.datetime(*[int(x) for x in
                                       (self.year[0], self.month[0],
                                        self.day[0], self.hour[0],
                                        self.minute[0], self.second[0])
                                       ]
                                     )
    return BufrFields


class DDBFile(DDBReader):

    """Allows for the decoding of Edition 4 BUFR files and encoding of this
    data in the WRF little_r format"""

    # Make a class derived from namedtuple that always have the requested
    # This could be a dictionary but i quite like that to be defined and fixed.
    # Although using a dictionary would remove the need of the translation dict

    BUFRFields = custum_namedtuple(pressure="pressure")

    def __init__(self, filename):
        BUFRReader.__init__(self, filename)
        self.filename = filename
        self.data = None
        self.time_filter = None
        self.region_filter = None
        # ensure that if no time or region limits are specified later, we can
        # still go ahead and include all observations
        self.set_time_filter()  # defaults => everything included
        self.set_region_filter_latlon()  # defaults => everything included

    def decode(self):
        """ Read the data from a BUFR file. """

        LOGGER.info(self.filename)

        data = []
        # LOOP OVER ALL MESSAGES
        while True:
            try:
                self.get_next_msg()
            except EOFError:
                break
            year = self.bufr_obj.ksec1[8]
            if year < 2000:
                year += 2000
            file_date = datetime.datetime(year, *self.bufr_obj.ksec1[9:14])
            if not self.time_filter(file_date):
                LOGGER.info("Date: {date:%Y%m%d_%H%M%S} is outside the desired "
                            "range. Ignoring the rest of this message"
                            .format(date=file_date))
                continue
            LOGGER.debug("Date: {date:%Y%m%d_%H%M%S}".format(date=file_date))

            # CHECK IF ALL THE REQUESTED DATA ARE AVAILABLE
            list_of_names = map(str.lower, self.get_names())
            trans_names = [x.bufr_name for x in self.BUFRFields._translate]
            if not set(trans_names).issubset(set(list_of_names)):
                raise InvalidMessageError()
                continue

            # LOOP OVER SUBSET AN EXTRACT DATA
            nsubsets = self.get_num_subsets()
            # get the indices of fileds we are interested in (there can be more
            # than one value for a certain name/field so put that in lists).
            descr_nrss = [[i for i, name in enumerate(list_of_names) if name == field]
                          for field in trans_names]

            # retreive all values as an array and store data of interest in
            # namedtuple. This seems to be faster than looping over the subsets
            # the python. Althougth it fail if bufr template uses delayed replication
            try:
                vals = self.get_values_as_2d_array()
                for subs in range(nsubsets):
                    data.append(self.BUFRFields(*[[vals[subs, descr_nr]
                                                   for descr_nr in descr_nrs]
                                                  for descr_nrs in descr_nrss]
                                                )
                                )
            except IncorrectUsageError:
                for subs in range(nsubsets):
                    data.append(self.BUFRFields(*[self.get_subset_values(subs)[descr_nr]
                                                  for descr_nr in descr_nrss]
                                                )
                                )
        self.data = data

    @staticmethod
    def _to_little_r_report(sub_data):
        """This method is supposed to be supplied by sub classes, the one
        defined here will give a little_r record with no data"""
        return LittleRReport().fromBUFR(
            datetime=sub_data.get_datetime(),
            latitude=sub_data.latitude[0],
            longitude=sub_data.longitude[0],
            pressure=sub_data.pressure[0]
        )

    def set_time_filter(self, start=None, stop=None):
        """Sets up the time_filter method according to desired start and stop
        times in %y%m%d%H format"""
        start_defined = False
        stop_defined = False
        if start is not None:
            start_time = datetime.datetime.strptime(start, '%y%m%d%H')
            start_defined = True
        if stop is not None:
            stop_time = datetime.datetime.strptime(stop, '%y%m%d%H')
            stop_defined = True

        def time_ok(date_time):
            """primordial ooze time filter function """
            if start_defined:
                if date_time < start_time:
                    return False
            if stop_defined:
                if date_time > stop_time:
                    return False
            return True

        self.time_filter = time_ok

    def set_region_filter_bmap(self, bmap):
        """Set up the region filter according to a supplied basemap object"""
        def latlon_ok(lat, lon):
            """amorphous region filter function"""
            projected_x, projected_y = bmap(lon, lat)
            return (bmap.xmin <= projected_x <= bmap.xmax) and \
                (bmap.ymin <= projected_y <= bmap.ymax)
        self.region_filter = latlon_ok

    def set_region_filter_latlon(self, west=-180, east=180, south=-90,
                                 north=90):
        """Set up region filter according to latitude, longitude limits"""
        def latlon_ok(lat, lon):
            """pre-crystallised region filter function"""
            return numpy.mod(lon, 360) > west and \
                numpy.mod(lon, 360) < numpy.mod(east, 360) and \
                lat > south and lat < north
        self.region_filter = latlon_ok

    def to_little_r(self):
        """produce little_r output, arguments let you filter the output to be
        within a lat-lon region and between start and stop (YYMMDDHH). If bmap
        is defined, instead use the given matplotlib.toolkits.basemap.Basemap
        object to determine the valid spatial range"""

        little_r_records = []
        for sub_data in self.data:
            date_time = sub_data.get_datetime()
            if (self.time_filter(date_time) and self.region_filter(sub_data.latitude[0], sub_data.longitude[0])):
                report = self._to_little_r_report(sub_data)
                if report is not None:
                    little_r_records.append(report)
        return little_r_records
