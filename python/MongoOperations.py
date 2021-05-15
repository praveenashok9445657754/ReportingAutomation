from pymongo import MongoClient
from pymongo import IndexModel, ASCENDING, DESCENDING
from datetime import datetime as dt
import numpy
import time
import datetime

FMT = '%H:%M:%S'
FMT_float = '%H:%M:%S.%f'

myclient = MongoClient('localhost', 27017)
dashboarding = myclient['dashboarding']
real_time_monitoring_status = dashboarding['real_time_monitoring_status']
job_info = dashboarding['job_info']
DeviceRecon = dashboarding['DeviceRecon']
RealTimeMonitoringStatus = dashboarding['real_time_monitoring_status']


class MongoOperations:
    def get_record_from_db(table_name, column_to_filter, column_to_filter_value, column_name_to_get):
        table = dashboarding[table_name]
        query = {column_to_filter: column_to_filter_value}
        mydoc = table.find(query, {column_name_to_get: 1, '_id': 0})
        lst = []
        for x in mydoc:
            lst.append(x[column_name_to_get])
        if len(lst) == 1:
            return lst[0]
        else:
            return lst

    def insert_record(table_name, query):
        table = dashboarding[table_name]
        table.insert_one(query)
        return None

    def update_record(table_name, where_condition_column, where_condition_value, to_be_updated_column,
                      to_be_updated_value):
        table = dashboarding[table_name]
        table.update({where_condition_column: where_condition_value},
                     {"$set": {to_be_updated_column: to_be_updated_value}})
        return None

    def get_max_record_from_db(process_name):
        table = dashboarding[process_name]
        latest_record = table.find({"date": "2020-05-17"}).sort("start_time", -1).limit(1)
        for i in latest_record:
            if i['status'] in ['Started', 'Completed', 'Failed', 'Retried']:
                return i['status']
            else:
                return False

    def get_latest_app_id(table_name, date):
        table = dashboarding[table_name]
        appl_id = table.find(
            {"$and": [{"date": date}, {"$or": [{"status": "Completed"}, {"status": "Failed"}]}]},
            {"application_id": 1, '_id': 0}).sort(
            "start_time", -1).limit(1)
        for i in appl_id:
            return i['application_id'].strip() if i['application_id'] is not None else print(None)

    def update_be_trends(table_name, appl_id, be_column_name, be_column_value):
        table = dashboarding[table_name]
        table.find_one_and_update({"$and": [{"date": "2020-05-17"}, {"application_id": appl_id}, {"status": "Failed"}]},
                                  {'$set': {be_column_name: be_column_value}},
                                  sort=[('start_time', DESCENDING)])

    def update_be_trends_for_after_failed(table_name, appl_id, be_column_name, be_column_value):
        table = dashboarding[table_name]
        table.update({"$and": [{"date": "2020-05-17"}, {"application_id": appl_id}, {"status": "Completed"}]},
                     {"$set": {be_column_name: be_column_value}})

    def update_final_output_count(table_name, output_count_value, date):
        table = dashboarding[table_name]
        table.update({"$and": [{"date": date}, {"status": "Completed"}]},
                     {"$set": {"output_count": output_count_value}})

    def update_application_id(table_name, date, application_id):
        table = dashboarding[table_name]
        table.update({"$and": [{"date": date}, {"status": "Started"}]},
                     {"$set": {"application_id": application_id}})

    def update_end_time(table_name, date, time_now):
        table = dashboarding[table_name]
        table.update(
            {"$and": [{"date": date}, {"$or": [{"status": "Completed"}, {"status": "Failed"}]}, {"end_time": None}]},
            {"$set": {"end_time": time_now}})

    def update_status(table_name, curr_status):
        table = dashboarding[table_name]
        for id in table.find().sort("$natural", -1).limit(1):
            latest_record = id['_id']
        table.update({"_id": latest_record}, {"$set": {"status": curr_status}})

    def return_count_of_records(table_name):
        table = dashboarding[table_name]
        return int(table.count())

    def check_for_todays_date_if_present(table_name, date):
        table = dashboarding[table_name]
        todays_date = table.find_one({"date": date})
        return todays_date

    def get_updated_flag_N_list(self):
        query = {'updated_flag': 'N'}
        list_of_process_id = RealTimeMonitoringStatus.find(query, {'process_id': 1, '_id': 0})
        lst =[]
        for i in list_of_process_id:
            lst.append(i['process_id'])
        return lst

    def get_updated_flag_Y_list(self):
        query = {'updated_flag': 'Y'}
        list_of_process_id = RealTimeMonitoringStatus.find(query, {'process_id': 1, '_id': 0})
        lst =[]
        for i in list_of_process_id:
            lst.append(i['process_id'])
        return lst

    def get_process_id_lesser_current_time(self):
        rows = job_info.find()
        time_now = datetime.datetime.now().time()
        lst_of_process_id = []
        for i in rows:
            time_string = i['start_time']
            process_time = dt.strptime(time_string, FMT).time()
            if time_now > process_time:
                lst_of_process_id.append(i['process_id'])
        return lst_of_process_id

    def get_all_from_job_info_and_insert_RealTimeMonitoringStatus():
        all_job_info = job_info.find({}, {'process_id': 1, 'process_name': 1, 'type': 1, 'start_time': 1, '_id': 0})
        status = 'Not Yet Started'
        updated_flag = 'N'
        date_now = '2020-05-17'#str(datetime.datetime.now().date())
        for all_job_info_dict in all_job_info:
            insert_document = {}
            insert_document['process_id'] = all_job_info_dict['process_id']
            insert_document['type'] = all_job_info_dict['type']
            insert_document['date'] = date_now
            insert_document['scheduled start_time'] = all_job_info_dict['start_time']
            insert_document['start_time'] = None
            insert_document['expected_end_time'] = None
            insert_document['end_time'] = None
            insert_document['process_name'] = all_job_info_dict['process_name']
            insert_document['status'] = status
            insert_document['updated_flag'] = updated_flag
            RealTimeMonitoringStatus.insert_one(insert_document)

    def update_updated_flag_Y(process_id, date):
        RealTimeMonitoringStatus.update(
            {"$and": [{"process_id": process_id}, {"date": date}]},
            {"$set": {"updated_flag": 'Y'}})

    def update_status_RealTimeMonitoringStatus(table_name, process_id,  date):
        table = dashboarding[table_name]
        latest_status = table.find({"date": date}, {"status": 1, '_id': 0}).sort("start_time", -1).limit(1)
        if int(latest_status.count()) > 0:
            for i in latest_status:
                RealTimeMonitoringStatus.update(
                    {"$and": [{"process_id": process_id}, {"date": date}]},
                    {"$set": {"status": i['status']}})

    def get_first_start_time(table_name, date):
        table = dashboarding[table_name]
        first_start_time = table.find({"date": date}, {"start_time": 1, '_id': 0}).sort("start_time", 1).limit(1)
        for i in first_start_time:
            return i['start_time']

    def update_first_start_time(process_id, date, start_time):
        RealTimeMonitoringStatus.update(
            {"$and": [{"process_id": process_id}, {"date": date}]},
            {"$set": {"start_time": start_time}})

    def get_last_end_time(table_name, date):
        table = dashboarding[table_name]
        last_end_time = table.find({"date": date}, {"end_time": 1, '_id': 0}).sort("start_time", -1).limit(1)
        for i in last_end_time:
            return i['end_time']

    def update_last_end_time(process_id, date, end_time):
        RealTimeMonitoringStatus.update(
            {"$and": [{"process_id": process_id}, {"date": date}]},
            {"$set": {"end_time": end_time}})

    def update_expected_end_time(table_name, process_id, date):
        table = dashboarding[table_name]
        sd = table.find(
            {"$and": [{"status": "Completed"}, {"$or": [{"number_of_retries": None}, {"number_of_retries": 0}]}]},
            {"start_time": 1, "end_time": 1, '_id': 0})
        time_lst = numpy.array([])
        for i in sd:
            time_diff = datetime.datetime.strptime(i['end_time'], '%H:%M:%S.%f') - datetime.datetime.strptime(
                i['start_time'], '%H:%M:%S.%f')
            time_lst = numpy.append(time_lst, [time_diff.total_seconds()])

        avg_of_time = numpy.average(time_lst)
        tme_str = time.strftime('%H:%M:%S', time.gmtime(avg_of_time))
        tme_tme = datetime.datetime.strptime(tme_str, '%H:%M:%S')
        first_strt_time = datetime.datetime.strptime(MongoOperations.get_first_start_time(table_name, date), FMT_float)
        dt1 = datetime.timedelta(hours=tme_tme.hour, minutes=tme_tme.minute, seconds=tme_tme.second,
                                 microseconds=tme_tme.microsecond)
        dt2 = datetime.timedelta(hours=first_strt_time.hour, minutes=first_strt_time.minute,
                                 seconds=first_strt_time.second, microseconds=first_strt_time.microsecond)
        fin = dt1 + dt2
        RealTimeMonitoringStatus.update(
            {"$and": [{"process_id": process_id}, {"date": date}]},
            {"$set": {"expected_end_time": str(fin)}})









