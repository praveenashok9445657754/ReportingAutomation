from StatusInfo import StatusInfo
from MongoOperations import MongoOperations
from RealTimeMonitoringStatus import RealTimeMonitoringStatus
from JobInfo import JobInfo
from Process import Processes
from datetime import datetime as dt
import datetime


class TablePopulating:

    def new_record_insertion(process_id, curr_date):
        jc_name = JobInfo.get_jc_name(process_id)
        curr_jc_status = StatusInfo.get_jc_status(curr_date, jc_name)
        Processes.insert_initial_process_detailss(process_id, curr_jc_status)

    if __name__ == "__main__":
        #MongoOperations.get_all_from_job_info_and_insert_RealTimeMonitoringStatus()
        process_id = "etl_01"
        FMT = '%H:%M:%S'
        curr_date = '2020-05-17'
        scheduled_time_string = JobInfo.get_scheduled_time(process_id)
        process_time = dt.strptime(scheduled_time_string, FMT).time()
        time_now = datetime.datetime.now().time()
        if Processes.check_for_todays_date_if_present(process_id) is None:
            new_record_insertion(process_id, curr_date)
        elif Processes.check_for_todays_date_if_present(process_id) is not None:
            if Processes.get_current_status(process_id) is False:
                new_record_insertion(process_id, curr_date)
        if Processes.get_current_status(process_id) in ['Started', 'Rerunning']:
            Processes.update_status(process_id)
        if Processes.get_current_status(process_id) == 'Started':
            Processes.update_application_id(process_id)
        elif Processes.get_current_status(process_id) == 'Completed':
            Processes.update_end_time(process_id)
            Processes.update_be_trends_for_after_failed(process_id)
            Processes.update_final_output_count(process_id)
            RealTimeMonitoringStatus.update_updated_flag_Y(process_id)
        elif Processes.get_current_status(process_id) == 'Failed':
            Processes.update_end_time(process_id)
            Processes.update_be_trends(process_id)
            if Processes.process_rerunning(process_id) is True:
                new_record_insertion(process_id, curr_date)
                Processes.update_application_id(process_id)
        # Monitoring dashboarding starts
        RealTimeMonitoringStatus.update_status_RealTimeMonitoringStatus(process_id)
        RealTimeMonitoringStatus.update_first_start_time(process_id)
        RealTimeMonitoringStatus.update_last_end_time(process_id)
        RealTimeMonitoringStatus.update_expected_end_time(process_id)
