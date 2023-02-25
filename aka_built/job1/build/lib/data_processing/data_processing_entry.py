from data_processing.data_processings import data_process
from datetime import datetime



def main(run_date, hour):

    print("Current Date of Run", run_date)

    start = datetime.now()
    print("whix_data_processing start time", start)

    job_1 = data_process()
    print(run_date)
    status = job_1.__whix_data_processing__(run_date, hour)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 crashed")

    print("whix_data_processing total time", datetime.now() - start)


def run(run_date, hour):

    main(run_date, hour)