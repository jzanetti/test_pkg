import cStringIO


missing = -888888
qc_flag = 0

class LittleRGeneric:
    missing = -888888
    qc_flag = 0

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def _process_kwargs(self, kwargs, errstr):
        for k in kwargs.keys():
            if k not in self.data:
                errstr = '%s: %s' % (errstr, k)
                raise (Exception), errstr
            else:
                self.data[k] = kwargs[k]

    def __str__(self):
        s = cStringIO.StringIO()
        self.to_file(s)
        return s.getvalue()


class LittleRDataRecord(LittleRGeneric):
    field_order = ['pressure', 'height', 'temperature', 'dew_point', 'speed',
                   'direction', 'u', 'v', 'rh', 'thickness']

    def __init__(self, **kwargs):
        self.data = {}
        for f in LittleRDataRecord.field_order:
            self.data[f] = LittleRGeneric.missing
            self.data[f + '_qc'] = LittleRGeneric.qc_flag
        self._process_kwargs(kwargs, 'Invalid LittleR data record field')

    def from_file(self, open_file):
        for f in LittleRDataRecord.field_order:
            self.data[f] = float(open_file.read(13))
            self.data[f + '_qc'] = int(open_file.read(7))
        open_file.read(1)
        return self

    def is_end_data_record(self):
        return self.data['pressure'] == -777777 and self.data['height'] == -777777

    def to_file(self, open_file):
        for f in LittleRDataRecord.field_order:
            open_file.write('%13.5f%7i' % (self.data[f], self.data[f + '_qc']))
        open_file.write('\n')


class LittleRReportHeader(LittleRGeneric):

    def __init__(self, **kwargs):
        self.data = {'latitude': LittleRGeneric.missing,
                     'longitude': LittleRGeneric.missing}
        for field in ['id', 'name', 'platform', 'source']:
            self.data[field] = ''
        self.data['elevation'] = LittleRGeneric.missing
        for field in ['num_vld_fld', 'num_error', 'num_warning', 'seq_num',
                      'num_dups']:
            self.data[field] = int(LittleRGeneric.missing)
        for field in ['is_sound', 'bogus', 'discard']:
            self.data[field] = ''
        self.data.update({'sut': int(LittleRGeneric.missing),
                          'julian': int(LittleRGeneric.missing),
                          'date_char': ''})
        for field in ['slp', 'ref_pres', 'ground_r', 'sst', 'psfc', 'precip', 't_max',
                      't_min', 't_min night', 'p_tend03', 'p_tend24', 'cloud_cvr',
                      'ceiling']:
            self.data[field] = LittleRGeneric.missing
            self.data[field + '_qc'] = LittleRGeneric.qc_flag
        self._process_kwargs(kwargs, 'Invalid LittleR header field')

    def from_file(self, open_file):
        """returns None if at end of open_file, or self if header successfully
        read"""
        self.data = {}
        s = open_file.read(20)
        if s == '':
            # then we are at the end of the file
            return None
        self.data['latitude'] = float(s)

        self.data['longitude'] = float(open_file.read(20))
        for field in ['id', 'name', 'platform', 'source']:
            self.data[field] = open_file.read(40)
        self.data['elevation'] = float(open_file.read(20))
        for field in ['num_vld_fld', 'num_error', 'num_warning', 'seq_num',
                      'num_dups']:
            self.data[field] = int(open_file.read(10))
        for field in ['is_sound', 'bogus', 'discard']:
            self.data[field] = open_file.read(10).strip()
        self.data['sut'] = int(open_file.read(10))
        self.data['julian'] = int(open_file.read(10))
        self.data['date_char'] = open_file.read(20).strip()
        for field in ['slp', 'ref_pres', 'ground_r', 'sst', 'psfc', 'precip', 't_max',
                      't_min', 't_min night', 'p_tend03', 'p_tend24', 'cloud_cvr',
                      'ceiling']:
            self.data[field] = float(open_file.read(13))
            self.data[field + '_qc'] = int(open_file.read(7))
        open_file.read(1)
        return self

    def to_file(self, open_file):
        open_file.write("%20.5f%20.5f" % (self.data['latitude'],
                                          self.data['longitude']))
        for field in ['id', 'name', 'platform', 'source']:
            open_file.write('%-40s' % self.data[field])
        open_file.write("%20.5f" % self.data['elevation'])
        for field in ['num_vld_fld', 'num_error', 'num_warning', 'seq_num',
                      'num_dups']:
            open_file.write('%10i' % self.data[field])
        for field in ['is_sound', 'bogus', 'discard']:
            open_file.write('%10s' % self.data[field])
        open_file.write('%10i%10i' % (self.data['sut'], self.data['julian']))
        open_file.write('%20s' % self.data['date_char'])
        for field in ['slp', 'ref_pres', 'ground_r', 'sst', 'psfc', 'precip', 't_max',
                      't_min', 't_min night', 'p_tend03', 'p_tend24', 'cloud_cvr',
                      'ceiling']:
            open_file.write('%13.5f%7i' % (self.data[field],
                                           self.data[field + '_qc']))
        open_file.write('\n')


class LittleREndReport(LittleRGeneric):

    def __init__(self, **kwargs):
        self.data = {'num_vld_fld': int(LittleRGeneric.missing),
                     'num_error': int(LittleRGeneric.missing),
                     'num_warning': int(LittleRGeneric.missing)}
        self._process_kwargs(kwargs, 'Invalid LittleR end record field')

    def from_file(self, open_file):
        self.data = {}
        for field in ['num_vld_fld', 'num_error', 'num_warning']:
            self.data[field] = int(open_file.read(7))
        open_file.read(1)
        return self

    def to_file(self, open_file):
        open_file.write("%7i%7i%7i\n" % (self.data['num_vld_fld'],
                                         self.data['num_error'],
                                         self.data['num_warning']))


class LittleRReport(LittleRGeneric):
    end_data_record = LittleRDataRecord(pressure=-777777,
                                        height=-777777)

    def __init__(self):
        self.header = LittleRReportHeader()
        self.data = []
        self.end_data_record = LittleRReport.end_data_record
        self.end_report = LittleREndReport()

    def fromDDB(self,**kwargs):
        
        allArguments = ['latitude', 'longitude', 'datetime', 'id', 'name',
                        'platform',
                        'source', 'elevation', 'num_vld_fld', 'num_error',
                        'num_warning', 'seq_num', 'num_dups', 'is_sound',
                        'bogus', 'discard', 'slp', 'pressure', 'direction',
                        'windspeed', 'speed_qc', 'direction_qc', 'height'];
        
        ddbData = {};
        for arg in allArguments:
            if arg in kwargs:
                ddbData[arg] = kwargs[arg]
            elif arg[-3:] == "_qc":
                ddbData[arg] = qc_flag;
            else:
                ddbData[arg] = missing;
    
    
        self.header = LittleRReportHeader(
                latitude=ddbData['latitude'],
                longitude=ddbData['longitude'],
                date_char=ddbData['datetime'].strftime('%Y%m%d%H%M%S'),
                id=ddbData['id'],
                name=ddbData['name'],
                platform=ddbData['platform'],
                source=ddbData['source'],
                elevation=ddbData['elevation'],
                num_vld_fld=ddbData['num_vld_fld'],
                num_error=ddbData['num_error'],
                num_warning=ddbData['num_warning'],
                seq_num=ddbData['seq_num'],
                num_dups=ddbData['num_dups'],
                is_sound=ddbData['is_sound'],
                bogus=ddbData['bogus'],
                discard=ddbData['discard'],
                slp=ddbData['slp']);

        self.data = [LittleRDataRecord(pressure=ddbData['pressure'],
                                       speed=ddbData['windspeed'],
                                       direction=ddbData['direction'],
                                       speed_qc=ddbData['speed_qc'],
                                       direction_qc=ddbData['direction_qc'],
                                       height=ddbData['height'])]
        num_vld_fld = 4
        for x in ['pressure', 'windspeed', 'direction', 'height']:
            if ddbData[x] == LittleRGeneric.missing:
                num_vld_fld -= 1
        self.end_report = LittleREndReport(num_vld_fld=num_vld_fld,
                                           num_error=ddbData['num_error'],
                                           num_warning=ddbData['num_warning'])
        return self
        
        
    def to_file(self, open_file):
        self.header.to_file(open_file)
        for dr in self.data:
            dr.to_file(open_file)
        self.end_data_record.to_file(open_file)
        self.end_report.to_file(open_file)

    def from_file(self, open_file):
        """returns None if at end of open_file, or self if header successfully
        read"""
        self.header = LittleRReportHeader().from_file(open_file)
        if self.header is None:
            return None
        self.data = []  # all data records except the end data record
        keep_reading = True
        while keep_reading:
            dr = LittleRDataRecord().from_file(open_file)
            if dr.is_end_data_record():
                keep_reading = False
                self.end_data_record = dr
            else:
                self.data.append(dr)
        self.end_report = LittleREndReport().from_file(open_file)
        return self
    