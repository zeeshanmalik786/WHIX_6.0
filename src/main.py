from data_processing.data_processing import data_process
from unified_processing.unified_processing import unified_process
from latent_processing.latent_processing import latent_process
from addition_adhoc.addition import additions
from cpe_devices.cpe_devices import device_names
from datetime import datetime, timedelta
from pytz import timezone


def main(run_date, hour):

    print("Current Date of Run", run_date)

    start = datetime.now()
    print("WHIX start time", run_date)

    # Adding more columns into the base table

    # job0 = additions()
    #
    # status = job0.__additions__()
    #
    # if status is True:
    #     print("Job 0 Done")
    # else:
    #     print("Job 0 Crashed")

    # job0 = device_names()
    #
    # status = job0.__device_names__(run_date)
    #
    # if status is True:
    #     print("Job 0 Done")
    # else:
    #     print("Job 0 Crashed")

    job1 = data_process()

    status = job1.__whix_data_processing__(run_date, hour)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 Crashed")

    job2 = unified_process()

    status = job2.__whix_unified_processing__(run_date, hour)

    if status is True:
        print("Job 2 Done")
    else:
        print("Job 2 Crashed")

    print("whix unification total time", datetime.now() - start)

    # job3 = latent_process()
    #
    # status = job3.__whix_latent_processing__(run_date)
    #
    # if status is True:
    #
    #     print("Job 3 Done")
    # else:
    #
    #     print("Job 3 Crashed")

if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('UTC')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=5)
    run_date = '2022-10-18'
    hour = datetime.now(tz).hour
    hour = 20
    print(hour)
    if hour == -1:
        hour = 23
    elif hour == 0:
        hour = 0
    print(run_date)
    main(run_date, 20)


