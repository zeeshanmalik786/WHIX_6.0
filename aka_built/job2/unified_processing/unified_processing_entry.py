from unified_processing.unified_processing import unified_process
from datetime import datetime



def main(run_date, hour):

    print("Current Date of Run", run_date)

    start = datetime.now()
    print("whix_unified_processing start time", start)

    job_1 = unified_process()
    status = job_1.__whix_unified_processing__(run_date)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 crashed")

    print("whix_unified_processing total time", datetime.now() - start)


def run(run_date, hour):

    main(run_date, hour)