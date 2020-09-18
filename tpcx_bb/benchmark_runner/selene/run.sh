#!/bin/bash

ACCOUNT=rapids
PARTITION=luna
WORKERS=0
#IMAGE=randerzander/selene:9-03_0
#IMAGE=nickb500/selene-tpcxbb:2020_09_11
#IMAGE=nickb500/selene-tpcxbb:2020_09_17
IMAGE=nickb500/selene-tpcxbb-small:2020_09_17

DATA_PATH="/lustre/fsw/rapids"
MOUNT_PATH="/tpcx-bb-data/"
TPCX_BB_HOME="$HOME/tpcx-bb"

rm *.out
rm -rf $HOME/dask-local-directory/*

srun \
    --account $ACCOUNT \
    --partition $PARTITION \
    --nodes 1 \
    --container-mounts $DATA_PATH:$MOUNT_PATH,$HOME:$HOME \
    --container-image=$IMAGE \
    bash -c "$TPCX_BB_HOME/tpcx_bb/benchmark_runner/selene/scheduler-client.sh"


#bash -c "$TPCX_BB_HOME/tpcx_bb/benchmark_runner/selene/scheduler-client.sh"
    #bash -c "ls $HOME"
    #bash -c "whoami" -- rgelhausen

if [ "$WORKERS" -gt "0" ]; then
	sbatch \
	    --account $ACCOUNT \
	    --partition $PARTITION \
	    --nodes $WORKERS
	    --container-mounts=$DATA_PATH:$MOUNT_PATH \
	    --container-image $IMAGE \
	    bash -c "$TPCX_BB_HOME/tpcx_bb/cluster_configuration/cluster-startup-selene.sh"
fi
