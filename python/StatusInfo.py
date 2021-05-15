import pandas as pd

#added comments
class StatusInfo:

    def get_jc_status(date, job_control_name):
        job_control_status = pd.read_csv("property_files/job_control_status.csv")
        job_control_status_row = job_control_status[job_control_status['Date'] == date]
        return job_control_status_row[job_control_name].get(2)

    def get_application_id(yarn_process_name):
        filename = "application_id_mapping.properties"
        return StatusInfo.get_value_from_properties(filename, yarn_process_name)

    def get_value_from_properties(filename, key):
        properties_mapping_dict = {}
        file = "property_files/" + filename
        properties_mapping = open(file, "r")
        for line in properties_mapping:
            name, value = line.split(":", 1)
            properties_mapping_dict[name.strip()] = value.strip()
        if key == False:
            return properties_mapping_dict
        else:
            return properties_mapping_dict[key]

    # def get_value_from_properties(filename):
    #     properties_mapping_dict = {}
    #     file = "property_files/" + filename
    #     properties_mapping = open(file, "r")
    #     for line in properties_mapping:
    #         name, value = line.split(":", 1)
    #         properties_mapping_dict[name.strip()] = value.strip()
    #     return properties_mapping_dict

    def get_output_count(process_name, date):
        filename = "output_data_count.properties"
        return StatusInfo.get_value_from_properties(filename, process_name)

    def check_if_the_process_rerunning(yarn_process_name):
        file = "property_files/" + "rerunning_applications.txt"
        rerunning_appl = open(file, "r")
        for i in rerunning_appl:
            if i.strip().lower() == yarn_process_name.strip().lower():
                return True
            else:
                return False

    def get_be_trends(application_id):
        filename = application_id + ".properties"
        be_trends = StatusInfo.get_value_from_properties(filename, key=False)
        return be_trends

    def get_be_trends_columns(process_id):
        file_name = "property_files/" + "BaseExtractColumns.txt"
        be_columns = open(file_name, "r")
        lst=[]
        for i in be_columns:
            lst.append(i.strip())
        return lst






