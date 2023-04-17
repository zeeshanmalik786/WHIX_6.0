from cpe_devices.cpe_devices import device_names
from datetime import datetime



def main(run_date):

    print("Current Date of Run", run_date)

    start = datetime.now()
    print("whix_device_names start time", start)

    job_1 = device_names()
    status = job_1.__device_names__(run_date)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 crashed")

    print("whix_device_names total time", datetime.now() - start)


def run(run_date):

    main(run_date)