import glob
import re
import os
import gc
import time

from prefect import Flow, task, Parameter
from prefect.environments import LocalEnvironment
from prefect.engine.executors import DaskExecutor

from distributed import Client, worker_client

dask_qnums = [str(i).zfill(2) for i in range(1, 31)]


def get_run_args(config_file):
    from xbb_tools.utils import add_empty_config
    import yaml

    with open(config_file) as fp:
        args = yaml.safe_load(fp.read())
    args = add_empty_config(args)
    return args


def prefect_pre_cluster_tasks(config, create_blazing_context=False):
    """Attaches to an existing cluster if available.
    By default, tries to attach to a cluster running on localhost:8786 (dask's default).

    This is currently hardcoded to assume the dashboard is running on port 8787.

    Optionally, this will also create a BlazingContext.
    """
    from xbb_tools.cluster_startup import (
        worker_count_info,
        _get_ucx_config,
        get_config_options,
    )

    def maybe_create_worker_directories(dask_worker):
        worker_dir = dask_worker.local_directory
        if not os.path.exists(worker_dir):
            os.mkdir(worker_dir)

    with worker_client() as client:
        client.run(maybe_create_worker_directories)

        # Get ucx config variables
        ucx_config = client.submit(_get_ucx_config).result()
        config.update(ucx_config)

        # Save worker information
        gpu_sizes = ["16GB", "32GB", "40GB"]
        worker_counts = worker_count_info(client, gpu_sizes=gpu_sizes)
        for size in gpu_sizes:
            key = size + "_workers"
            if config.get(key) is not None and config.get(key) != worker_counts[size]:
                print(
                    f"Expected {config.get(key)} {size} workers in your cluster, but got {worker_counts[size]}. It can take a moment for all workers to join the cluster. You may also have misconfigred hosts."
                )
                sys.exit(-1)

        config["16GB_workers"] = worker_counts["16GB"]
        config["32GB_workers"] = worker_counts["32GB"]
        config["40GB_workers"] = worker_counts["40GB"]

        bc = None
        if create_blazing_context:
            bc = BlazingContext(
                dask_client=client,
                pool=os.environ.get("BLAZING_POOL", False),
                network_interface=os.environ.get("INTERFACE", "ib0"),
                config_options=get_config_options(),
                allocator=os.environ.get("BLAZING_ALLOCATOR_MODE", "managed"),
                initial_pool_size=os.environ.get("BLAZING_INITIAL_POOL_SIZE", None),
            )

    return bc


def load_query(qnum, fn):
    import importlib, types

    loader = importlib.machinery.SourceFileLoader(
        qnum, f"queries/q{qnum}/tpcx_bb_query_{qnum}.py"
    )
    mod = types.ModuleType(loader.name)
    loader.exec_module(mod)
    return mod.main


@task(log_stdout=True)
def query_prep(config_file_path):
    config = get_run_args(config_file_path)
    include_blazing = config.get("benchmark_runner_include_bsql")
    bc = prefect_pre_cluster_tasks(config, create_blazing_context=include_blazing)
    return {"config": config, "bc": bc}


if __name__ == "__main__":
    from xbb_tools.cluster_startup import import_query_libs, attach_to_cluster
    from xbb_tools.utils import run_query

    dask_queries = {
        qnum: load_query(qnum, f"queries/q{qnum}/tpcx_bb_query_{qnum}.py")
        for qnum in dask_qnums
    }
    base_path = os.getcwd()

    @task(log_stdout=True)
    def run_prefect_query(config, qnum, base_path):
        qpath = f"{base_path}/queries/q{qnum}/"
        os.chdir(qpath)

        with worker_client() as client:
            t1 = time.time()
            run_query(config=config, client=client, query_func=dask_queries.get(qnum))
            return time.time() - t1

        os.chdir(base_path)

    if __name__ == "__main__":
        executor = DaskExecutor(address=os.environ["Scheduler_address"])
        environment = LocalEnvironment(
            executor=executor,
        )

        curr_path = os.getcwd()
        os.chdir(curr_path)
        with Flow("TPCx-BB Flow", environment=environment) as tpcx_bb_flow:
            config_file_path = Parameter(
                name="config_file", default="benchmark_runner/benchmark_config.yaml"
            )

            qpep_res = query_prep(config_file_path)
            config = qpep_res["config"]
            results = []
            for q in dask_qnums:
                res = run_prefect_query(
                    config=config,
                    qnum=q,
                    base_path=curr_path,
                    task_args=dict(name=f"Query_{q}"),
                )

                if len(results) > 0:
                    res.set_upstream(results[-1])

                results.append(res)
        tpcx_bb_flow.register(project_name="Tpcx-bb-rapids")
        #tpcx_bb_flow.run(executor=executor)
