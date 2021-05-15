from MongoOperations import MongoOperations
from JobInfo import JobInfo
from StatusInfo import StatusInfo
import datetime

REAL_TIME_MONITORING_STATUS = "real_time_monitoring_status"
JOB_INFO = "job_info"
DEVICE_RECON = "DeviceRecon"


class Processes:

    def add_key_value(self, key, value):
        self[key] = value

    def get_current_status(process_id):
        table_name = JobInfo.get_process_name(process_id)
        return str(MongoOperations.get_max_record_from_db(table_name))

    def get_updated_application_id(table_name):
        curr_date = '2020-05-17'#str(datetime.datetime.now().date())
        return MongoOperations.get_latest_app_id(table_name, curr_date)

    def insert_initial_process_detailss(process_id, jc_status):
        insert_document = {}
        insert_document["date"] = "2020-05-17"#str(datetime.datetime.now().date())
        insert_document["process_id"] = process_id
        insert_document["process_name"] = JobInfo.get_process_name(process_id)
        insert_document["status"] = jc_status
        for i in StatusInfo.get_be_trends_columns(process_id):
            insert_document[i] = None
        insert_document["output_count"] = None
        insert_document["application_id"] = None
        insert_document["start_time"] = str(datetime.datetime.now().time())
        insert_document["end_time"] = None
        insert_document["number_of_retries"] = None
        insert_document["list_of_alerts"] = None
        insert_document["failed_reasons"] = None
        MongoOperations.insert_record(DEVICE_RECON, insert_document)
        return None

    def update_be_trends(process_id):
        table_name = JobInfo.get_process_name(process_id)
        appl_id = Processes.get_updated_application_id(table_name)
        # if appl_id is not None:
        #     appl_id = "application_id_01" #delete thi after testing.
        be_trends_dict = StatusInfo.get_be_trends(appl_id)
        for be_column_name, be_column_value in be_trends_dict.items():
            MongoOperations.update_be_trends(table_name, appl_id, be_column_name, be_column_value)

    def update_be_trends_for_after_failed(process_id):
        table_name = JobInfo.get_process_name(process_id)
        appl_id = Processes.get_updated_application_id(table_name)
        # if appl_id is not None:
        #     appl_id = "application_id_01" #delete thi after testing.
        be_trends_dict = StatusInfo.get_be_trends(appl_id)
        for be_column_name, be_column_value in be_trends_dict.items():
            MongoOperations.update_be_trends_for_after_failed(table_name, appl_id, be_column_name, be_column_value)

    def update_final_output_count(process_id):
        table_name = JobInfo.get_process_name(process_id)
        process_name = JobInfo.get_process_name(process_id)
        date = '2020-05-17'#str(datetime.datetime.now().date())
        output_count = StatusInfo.get_output_count(process_name, date)
        MongoOperations.update_final_output_count(table_name, output_count, date)

    def update_application_id(process_id):
        table_name = JobInfo.get_process_name(process_id)
        yarn_process_name = JobInfo.get_yarn_process_name(process_id)
        appl_id = StatusInfo.get_application_id(yarn_process_name)
        MongoOperations.update_application_id(table_name, "2020-05-17", appl_id)    #str(datetime.datetime.now().date())

    def update_end_time(process_id):
        table_name = JobInfo.get_process_name(process_id)
        MongoOperations.update_end_time(table_name, '2020-05-17',#str(datetime.datetime.now().date()),
                                        str(datetime.datetime.now().time()))

    def update_status(process_id):
        table_name = JobInfo.get_process_name(process_id)
        jc_name = JobInfo.get_jc_name(process_id)
        curr_status = StatusInfo.get_jc_status("2020-05-17", jc_name)   #str(datetime.datetime.now().date()
        MongoOperations.update_status(table_name, curr_status)

    def process_rerunning(process_id):
        yarn_process_name = JobInfo.get_yarn_process_name(process_id)
        return StatusInfo.check_if_the_process_rerunning(yarn_process_name)

    def return_count_of_records(process_id):
        table_name = JobInfo.get_process_name(process_id)
        MongoOperations.return_count_of_records(table_name)

    def check_for_todays_date_if_present(process_id):
        table_name = JobInfo.get_process_name(process_id)
        date = "2020-05-17" #str(datetime.datetime.now().date()
        return MongoOperations.check_for_todays_date_if_present(table_name, date)

