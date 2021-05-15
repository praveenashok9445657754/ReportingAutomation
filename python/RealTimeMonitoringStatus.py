from datetime import datetime as dt
from JobInfo import JobInfo
from MongoOperations import MongoOperations
import RealTimeMonitoringStatus
import datetime
FMT = '%H:%M:%S'

class RealTimeMonitoringStatus:

    def remove_duplicate(list_with_duplicates):
        return list(dict.fromkeys(list_with_duplicates))

    def get_running_processes(process_id):
        process_having_updated_flag_N = MongoOperations.get_updated_flag_N_list()
        process_having_updated_flag_Y = MongoOperations.get_updated_flag_Y_list()
        process_having_time_lesser_than_timenow = MongoOperations.get_process_id_lesser_current_time()
        combined_running_process = RealTimeMonitoringStatus.remove_duplicate(process_having_time_lesser_than_timenow + process_having_updated_flag_N)
        running_process = []
        for i in combined_running_process:
            if i not in process_having_updated_flag_Y:
                running_process.append(i)
        return running_process

    def insert_record_into_RealTimeMonitoringStatus(self):
        MongoOperations.get_all_from_job_info_and_insert_RealTimeMonitoringStatus()

    def update_status_RealTimeMonitoringStatus(process_id):
        date_now = '2020-05-17'#str(datetime.datetime.now().date())
        table_name = JobInfo.get_process_name(process_id)
        MongoOperations.update_status_RealTimeMonitoringStatus(table_name, process_id, date_now)

    def update_updated_flag_Y(process_id):
        date_now = '2020-05-17'#str(datetime.datetime.now().date())
        MongoOperations.update_updated_flag_Y(process_id, date_now)

    def update_first_start_time(process_id):
        date_now = '2020-05-17'  # str(datetime.datetime.now().date())
        table_name = JobInfo.get_process_name(process_id)
        first_start_time = MongoOperations.get_first_start_time(table_name, date_now)
        MongoOperations.update_first_start_time(process_id, date_now, first_start_time)

    def update_last_end_time(process_id):
        date_now = '2020-05-17'  # str(datetime.datetime.now().date())
        table_name = JobInfo.get_process_name(process_id)
        last_end_time = MongoOperations.get_last_end_time(table_name, date_now)
        MongoOperations.update_last_end_time(process_id, date_now, last_end_time)

    def update_expected_end_time(process_id):
        date_now = '2020-05-17'  # str(datetime.datetime.now().date())
        table_name = JobInfo.get_process_name(process_id)
        MongoOperations.update_expected_end_time(table_name, process_id, date_now)


