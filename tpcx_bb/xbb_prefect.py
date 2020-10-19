import glob
import re
import os
import gc
import time

from prefect import Flow, task
from distributed import Client

dask_qnums = [str(i).zfill(2) for i in range(1, 31)]


def load_query(qnum, fn):
    import importlib, types
    loader = importlib.machinery.SourceFileLoader(qnum, f"queries/q{qnum}/tpcx_bb_query_{qnum}.py")
    mod = types.ModuleType(loader.name)
    loader.exec_module(mod)
    return mod.main

if __name__ == "__main__":
    from xbb_tools.cluster_startup import import_query_libs, attach_to_cluster
    from xbb_tools.utils import tpcxbb_argparser, run_query
    
    dask_queries = {
        qnum: load_query(qnum, f"queries/q{qnum}/tpcx_bb_query_{qnum}.py")
        for qnum in dask_qnums
    }
    base_path = os.getcwd()
    
    @task(log_stdout=True)
    def query_prep():
        config = tpcxbb_argparser()
        include_blazing = config.get("benchmark_runner_include_bsql")
        client, bc = attach_to_cluster(config, create_blazing_context=include_blazing)
        return {"config": config, "client": client, "bc": bc}
    
    @task(log_stdout=True)
    def run_prefect_query(config, client, qnum):
        t1 = time.time()
        run_query(config=config, client=client, query_func=dask_queries.get(qnum))
        return (time.time() - t1)
    
    if __name__ == "__main__":
        with Flow("TPCx-BB Flow") as tpcx_bb_flow:
            results = []
            qpep_res = query_prep()
            for q in dask_qnums:
                t = run_prefect_query(config=qpep_res["config"], 
                                        client=qpep_res["client"],
                                        qnum=q,
                                        )
                results.append(t)
        tpcx_bb_flow.visualize()
        tpcx_bb_flow.run()

                             