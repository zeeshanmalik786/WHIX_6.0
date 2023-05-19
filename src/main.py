from data_processing.data_processing import data_process
from unified_processing.unified_processing import unified_process
from cpe_devices.cpe_devices import device_names
from behind_pods.behind_pods import behind_pods
from datetime import datetime, timedelta
from pytz import timezone
from scoring.scoring import scoring


def main(run_date, hour):

    print("Current Date of Run", run_date)

    print("WHIX start time", run_date)

    # job1 = data_process()
    #
    # status = job1.__whix_data_processing__(run_date, hour)
    #
    # if status is True:
    #     print("Job 1 Done")
    # else:
    #     print("Job 1 Crashed")
    #
    # job2 = unified_process()
    #
    # status = job2.__whix_unified_processing__(run_date)
    #
    # if status is True:
    #     print("Job 2 Done")
    # else:
    #     print("Job 2 Crashed")

    job3 = scoring()

    status = job3.__score__(run_date, hour)

    if status is True:
        print("Job 3 Done")
    else:
        print("Job 3 Crashed")


if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('UTC')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=1)
    hour = datetime.now(tz).hour
    print(hour)
    if hour == -1:
        hour = 23
    elif hour == 0:
        hour = 0
    print(run_date)
    # Manually Debugging the Code

    main(datetime.date(run_date), hour)


