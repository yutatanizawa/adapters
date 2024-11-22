import argparse
import os 

parser = argparse.ArgumentParser(description="Process job parameters")
parser.add_argument("--job_path", type=str, required=True, help="Path to the job script")
parser.add_argument("--executor", type=str, required=True, help="Executor to be used")
parser.add_argument("--stdout_path", type=str, required=True, help="path of the stdout")
parser.add_argument("--stderr_path", type=str, required=True, help="path of the stderr")
parser.add_argument("--duration", type=str, required=False, help="Walltime")
parser.add_argument("--queue_name", type=str, required=True, help="Name of the queue")
parser.add_argument("--project_name", type=str, required=False, help="")
parser.add_argument("--reservation_id", type=str, required=False, help="")
parser.add_argument("--exclusive_node_use", type=bool, required=False, help="")
parser.add_argument("--node_count", type=int, required=False, help="")
parser.add_argument("--memory_per_node", type=int, required=False, help="")
parser.add_argument("--gpu_cores_per_process", type=int, required=False, help="")


args = parser.parse_args()

from psij import Job, JobSpec, JobExecutor, JobAttributes, ResourceSpecV1
from pathlib import Path

args.job_path = os.path.expanduser(args.job_path)

ex = JobExecutor.get_instance(args.executor)

attributes = JobAttributes(queue_name=args.queue_name)
if args.duration:
    attributes.duration = JobAttributes.parse_walltime(args.duration)
if args.project_name:
    attributes.project_name = args.project_name
if args.reservation_id:
    attributes.reservation_id = args.reservation_id

resources = ResourceSpecV1()
if not args.exclusive_node_use:
    resources.exclusive_node_use = False
if args.node_count:
    resources.node_count = args.node_count
if args.memory_per_node:
    resources.memory_per_node = args.memory_per_node
if args.gpu_cores_per_process:
    resources.gpu_cores_per_process = args.gpu_cores_per_process

job = Job(
    JobSpec(
        executable=args.job_path,
        stdout_path = Path(args.stdout_path),
        stderr_path = Path(args.stderr_path),
        attributes=attributes,
        resources=resources,
    )
)

ex.submit(job)
print(job.native_id)