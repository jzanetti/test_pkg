import obs2r_related
import obs2aws_related
import amv_little_r
import datetime


filename = '/home/szhang/workspace/obs2r_amv/testdata/amv_20120214010000_IXCN41.bufr'
#filename = '/home/szhang/Downloads/obs2r/OBS2r-master/test/data/amv/edition4/amv_IXCN41_20120214010000.bufr';
domain_range = [138,159,-60,-30];
time_range = ['2012021401','2012021401'];

#little_r_reports = obs2r_related.amv2little_r(filename,domain_range,time_range);

ddb_rows = obs2aws_related.amv2dynamoDB(filename,'amv');
for i in range(0,len(ddb_rows)):
    cur_row = ddb_rows[i];
    little_r_records = [];
    a = amv_little_r.LittleRReport().fromDDB(datetime=datetime.datetime(int(cur_row['datetime']['S'][0:4]),int(cur_row['datetime']['S'][4:6]),int(cur_row['datetime']['S'][6:8]),cur_row['datetime']['S'][8:10],cur_row['datetime']['S'][10:12],cur_row['datetime']['S'][12:14]),
                                            latitude=float(cur_row['latitude']['N']),
                                            longitude=float(cur_row['longitude']['N']),
                                            pressure=85000.0,
                                            direction=351.0,
                                            windspeed=float(cur_row['windDirection']['N']),
                                            speed_qc=95.0,
                                            direction_qc=96.0,
                                            id="-7777", name="MTSAT-1R", platform="FM-88 SATOB",
                                            source="BoM feed", num_vld_fld=3, seq_num=1, is_sound="T",
                                            bogus="F", discard="F");


print a
'''
ddb_sample_dic = {'direction': 351.0, 'bogus': 'F', 'name': 'MTSAT-1R', 'platform': 'FM-88 SATOB', 'seq_num': 1, 'speed_qc': 95.0, 'longitude': 153.5, 'datetime': datetime.datetime(2012, 2, 14, 0, 31), 'pressure': 85000.0, 'windspeed': 14.5, 'is_sound': 'T', 'direction_qc': 96.0, 'latitude': 54.0, 'discard': 'F', 'num_vld_fld': 3, 'id': '-7777', 'source': 'BoM feed'};
#amv_little_r.fromDDB(ddb_sample_dic);
amv_little_r.LittleRReport().fromDDB(datetime=datetime.datetime(2012,2,14,0,31,0),
                                        latitude=54.0,
                                        longitude=153.5,
                                        pressure=85000.0,
                                        direction=351.0,
                                        windspeed=14.5,
                                        speed_qc=95.0,
                                        direction_qc=96.0,
                                        id="-7777", name="MTSAT-1R", platform="FM-88 SATOB",
                                        source="BoM feed", num_vld_fld=3, seq_num=1, is_sound="T",
                                        bogus="F", discard="F");

little_r_reports = obs2r_related.amv2little_r(filename,domain_range,time_range);
print little_r_reports
obs2aws_related.amv2dynamoDB(filename,'amv');
#little_r_reports = obs2r_related.amv2little_r(filename,domain_range,time_range);
'''
print 'done'