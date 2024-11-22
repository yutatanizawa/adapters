import argparse
import json

parser = argparse.ArgumentParser(description="Process job parameters")
parser.add_argument("--id", type=str, help="Path to the job script")
# parser.add_argument("--owner", type=str, help="the name of job owner")
parser.add_argument("--executor", type=str, required=True, help="Executor to be used")

args = parser.parse_args()

from psij import Job, JobExecutor

ex = JobExecutor.get_instance(args.executor)
if args.id:
    job = Job()
    job._native_id = args.id
    job_data = ex.info(job)
else:
    job_data = ex.info()

print(json.dumps(job_data))