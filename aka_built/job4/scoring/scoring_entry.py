from scoring.scoring import scoring_calculation
from datetime import datetime



def main(run_date, hour):

    print("Current Date of Run", run_date)

    start = datetime.now()
    print("whix_scoring start time", start)

    job_1 = scoring_calculation()
    status = job_1.__score__(run_date, hour)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 crashed")

    print("whix_scoring total time", datetime.now() - start)


def run(run_date, hour):

    main(run_date, hour)