from OBS2r import amv, bufr
from OBS2r import little_r

little_r_reports = {    'vis': little_r.LittleRReportList(),
                        'ir': little_r.LittleRReportList(),
                        'wv': little_r.LittleRReportList()
                    }

def amv2little_r(amv_filepath, domain_range, time_range):
    amv_fd = amv.AMVFile(amv_filepath);
    #amv_fd.set_region_filter_latlon(west=domain_range[0], east=domain_range[1], south=domain_range[2], north=domain_range[3]);
    #amv_fd.set_time_filter(start='12021401',stop='12021403');
    amv_fd.decode();
    band = amv_fd._get_band();
    little_r_reports[band].extend(amv_fd.to_little_r())
    return little_r_reports;
