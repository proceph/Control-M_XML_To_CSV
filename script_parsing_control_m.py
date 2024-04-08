import pandas as pd
import xml.etree.ElementTree as ET
import os

def getAttributes(tree_value, attrib_value):
    try:
        return tree_value.attrib[attrib_value]
    except KeyError:
        return ('none')
    
def parseJob(tree_folder_type, tree_root):
    for folder in tree_root.findall(tree_folder_type):
        folder_name = getAttributes(folder, 'FOLDER_NAME')
        for job in folder.findall('JOB'):
            job_name = getAttributes(job,'JOBNAME')
            job_creation_time = getAttributes(job, 'CREATION_DATE')
            job_description = getAttributes(job,'DESCRIPTION')
            job_week_days = getAttributes(job,'WEEKDAYS')
            job_appl_type = getAttributes(job,'APPL_TYPE')
            job_task_type = getAttributes(job, 'TASKTYPE')
            job_embedded_script = getAttributes(job, 'USE_INSTREAM_JCL')
            job_working_dir = getAttributes(job, 'MEMLIB')
            job_script_filename = getAttributes(job, 'MEMNAME')
            job_script = job_working_dir + job_script_filename
            job_cmdline = getAttributes(job,'CMDLINE')
            job_instream_jcl = getAttributes(job,'INSTREAM_JCL')
            job_nodeid = getAttributes(job,'NODEID')
            job_run_as = getAttributes(job, 'RUN_AS')
            ssm_commands = ''
            ssm_working_dir = ''
            if job_appl_type == "OS":
                if job_task_type == "Command":
                    ssm_commands = job_cmdline
                else:
                    if job_task_type == "Job" and job_embedded_script == "N" :
                        ssm_commands = job_script_filename
                        ssm_working_dir = job_working_dir
                    else:
                        if job_task_type == "Job" and job_embedded_script == "Y" :
                            ssm_commands = job_instream_jcl

            job_incond = ''
            for incond in job.findall('INCOND'):
                job_incond += getAttributes(incond,'NAME') + ' | '
            job_outcond =''
            for outcond in job.findall('OUTCOND'):
                job_outcond += getAttributes(outcond, 'NAME') + ' | '
            job_variables = ''
            ssm_file_to_watch = ''
            for variable in job.findall('VARIABLE'):
                job_variables += getAttributes(variable,'NAME') + "=" + getAttributes(variable,'VALUE') + ' | '
                if getAttributes(variable,'NAME') == "%%FileWatch-FILE_PATH":
                    ssm_file_to_watch = getAttributes(variable,'VALUE')

            job_email_cond = ''
            job_email_urg = ''
            job_email_dest = ''
            for on in job.findall('ON'):
                job_email_cond = getAttributes(on, 'CODE')
                for domail in on.findall('DOMAIL'):
                    job_email_urg = getAttributes(domail, 'URGENCY')
                    job_email_dest = getAttributes(domail, 'DEST')


            rows.append({"CREATION_DATE": job_creation_time, 
                        "FOLDER_TYPE": tree_folder_type,
                        "FOLDER_NAME": folder_name,  
                        "JOB_NAME": job_name,
                        "JOB_DESCRIPTION": job_description, 
                        "JOB_APPL_TYPE": job_appl_type, 
                        "JOB_TASK_TYPE": job_task_type,
                        "JOB_EMBEDDED_SCRIPT" : job_embedded_script,
                        "JOB_SCRIPT": job_script,
                        "JOB_INSTREAM_JCL": job_instream_jcl,
                        "JOB_CMDLINE": job_cmdline,
                        "SSM_WORKING_DIR": ssm_working_dir,
                        "SSM_COMMANDS": ssm_commands,
                        "SSM_FILE_TO_WATCH": ssm_file_to_watch,
                        "JOB_NODEID": job_nodeid,
                        "JOB_RUN_AS": job_run_as,
                        "JOB_WEEKDAYS": job_week_days,
                        "JOB_INCOND": job_incond,
                        "JOB_OUTCOND": job_outcond,  
                        "JOB_VARIABLES": job_variables,
                        "JOB_EMAIL_COND": job_email_cond,
                        "JOB_EMAIL_URG": job_email_urg,
                        "JOB_EMAIL_DEST": job_email_dest
                        })


#Get current directory
current_directory = os.getcwd()

#Prepare columns and rows
cols = ["CREATION_DATE",
        "FOLDER_TYPE",
        "FOLDER_NAME",
        "JOB_NAME",
        "JOB_DESCRIPTION",
        "JOB_APPL_TYPE",
        "JOB_TASK_TYPE",
        "JOB_EMBEDDED_SCRIPT",
        "JOB_SCRIPT",
        "JOB_INSTREAM_JCL",
        "JOB_CMDLINE",
        "SSM_WORKING_DIR", 
        "SSM_COMMANDS",
        "SSM_FILE_TO_WATCH",
        "JOB_NODEID", 
        "JOB_RUN_AS",
        "JOB_WEEKDAYS",  
        "JOB_INCOND",
        "JOB_OUTCOND",
        "JOB_VARIABLES",
        "JOB_EMAIL_COND",
        "JOB_EMAIL_URG",
        "JOB_EMAIL_DEST"
        ]
rows = []

#Create the tree for parsing elements
tree = ET.parse(current_directory + "\\dump.xml")
root = tree.getroot()

#Parse Folder type and update rows
parseJob('SMART_FOLDER', root)
parseJob('FOLDER', root)

#Write Data and export file into .csv
df = pd.DataFrame(rows, columns=cols)
df.to_csv(current_directory +  "\\output.csv", index=False)