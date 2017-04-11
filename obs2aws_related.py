from obs2aws import tools
import dynamodb_related

AMV_FIELD_LISTS = { "required":["satelliteIdentifier",
                                 "year", "month",
                                 "day", "hour", "minute", "second",
                                 "latitude", "longitude", 'pressure','percentConfidence',
                                 "windDirection", "windSpeed"],
                    "meteorology": []}

def amv2dynamoDB(amv_filepath,report_type):
    _, timestamp, gtsheader, file_ext = tools.parse_filename(amv_filepath);
    bounding_box, times, ddb_rows = dynamodb_related.get_s3keyinfo_ddb_rows(amv_filepath,AMV_FIELD_LISTS,report_type,timestamp);
    return ddb_rows;