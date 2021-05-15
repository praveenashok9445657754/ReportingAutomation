from MongoOperations import MongoOperations

REAL_TIME_MONOTORING_STATUS = "real_time_monitoring_status"
JOB_INFO = "job_info"
DEVICE_RECON = "DeviceRecon"


class JobInfo:
    def get_jc_name(process_id):
        return MongoOperations.get_record_from_db(JOB_INFO, "process_id", process_id, "job_control_name")

    def get_process_name(process_id):
        return MongoOperations.get_record_from_db(JOB_INFO, "process_id", process_id, "process_name")

    def get_scheduled_time(process_id):
        return MongoOperations.get_record_from_db(JOB_INFO, "process_id", process_id, "start_time")

    def get_yarn_process_name(process_id):
        return MongoOperations.get_record_from_db(JOB_INFO, "process_id", process_id, "yarn_process_name")
