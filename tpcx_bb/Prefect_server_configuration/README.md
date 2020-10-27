## Running the Tpcx-bb workflow as a Prefect flow

### Prerequisites
- Ensure that Prefect in installed in the environment `conda install -c conda-forge prefect`
- Ensure that the dask-cuda cluster is setup following the instructions [here](../../README.md#starting-your-cluster)

### Flow composition
The flow comprises of 1 parameter and 31 tasks.
- The `config_file` parameter is a path the the benchmark_config.yaml file used by the query runner to get relevant configurations.
- The `query_prep` task for performing pre query execution tasks such as getting cluster information, importing required libraries on all workers etc.
- One task for each of the 30 Tpcx-bb queries each reposible for running a query end to end.

### Running the Flow
- One off flow runs with Prefect
  - Use `flow.run` [here](../xbb_prefect.py#L151) instead of `flow.register`
- For setting up a Prefect server and monitoring flows, setting intervals etc. 
    - Use the prefect server startup [script](prefect_server_startup.sh) as following
      - `bash prefect_server_start.sh`
  - Alternatively, follow the instruction in the [Prefect docs](https://docs.prefect.io/orchestration/tutorial/configure.html#configure-your-environment) to setup a server accordingly.
  - Run the [xbb_prefect](../xbb_prefect.py) file to register the flow the the prefect server. 
    - `python xbb_prefect.py`
  - Run the flow or setup a schedule from UI (usually port 8080), CLI or python following the instructions [here](https://docs.prefect.io/orchestration/tutorial/first.html#run-flow-with-the-prefect-api).
