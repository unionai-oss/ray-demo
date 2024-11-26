from flytekit import ImageSpec, Resources, task, workflow
from flytekitplugins.ray import HeadNodeConfig, RayJobConfig, WorkerNodeConfig
from ray_helper import ProgressActor, sampling_task
import ray
import time

container_image = ImageSpec(
    name="ray-union-demo",
    python_version="3.11.9",
    apt_packages=["wget", "gdb"],
    requirements="requirements.txt",
    registry="ghcr.io/fiedlerNr9",
)

ray_config = RayJobConfig(
    head_node_config=HeadNodeConfig(
        ray_start_params={"num-cpus": "0", "log-color": "true"}
    ),
    worker_node_config=[
        WorkerNodeConfig(
            group_name="ray-group", replicas=0, min_replicas=0, max_replicas=5
        )
    ],
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=120,
    enable_autoscaling=True,
)


@task(
    task_config=ray_config,
    requests=Resources(mem="4Gi", cpu="2"),
    container_image=container_image,
    environment={"PYTHONMALLOC": "malloc"}, # You may want this for memray profiling in the ray dashboard
)
def approximate_pi_distributed(num_sampling_tasks: int, num_samples_per_task: int):

    total_num_samples = num_sampling_tasks * num_samples_per_task
    # Create the progress actor.
    progress_actor = ProgressActor.remote(total_num_samples)
    # Create and execute all sampling tasks in parallel.
    results = [
        sampling_task.remote(num_samples_per_task, i, progress_actor)
        for i in range(num_sampling_tasks)
    ]
    # Query progress periodically.
    while True:
        progress = ray.get(progress_actor.get_progress.remote())
        print(f"Progress: {int(progress * 100)}%")

        if progress == 1:
            break

        time.sleep(0.3)

    # Get all the sampling tasks results.
    total_num_inside = sum(ray.get(results))
    pi = (total_num_inside * 4) / total_num_samples
    print(f"Estimated value of π is: {pi}")


@workflow
def wf(num_sampling_tasks: int = 1000, num_samples_per_task: int = 10_000_000):
    approximate_pi_distributed(
        num_sampling_tasks=num_sampling_tasks, num_samples_per_task=num_samples_per_task
    )
