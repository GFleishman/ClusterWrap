from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import dask.config
from pathlib import Path
import os
import sys
import time

SCALE_DELAY = 30


class _cluster(object):

    def __init__(self):
        self.client = None
        self.cluster = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.client is not None:
            self.client.close()
        if self.cluster is not None:
            self.cluster.close()

    def set_cluster(self, cluster):
        self.cluster = cluster
    def set_client(self, client):
        self.client = client

    def scale_cluster(self, nworkers):
        self.cluster.scale(jobs=nworkers)
        # wait a little whlie for workers
        print(f"Waiting {SCALE_DELAY} seconds for cluster to scale")
        time.sleep(SCALE_DELAY)
        print("Cluster wait time complete")

    def modify_dask_config(self, options):
        dask.config.set(options)

    def get_dashboard(self):
        if self.cluster is not None:
            return self.cluster.dashboard_link


class janelia_lsf_cluster(_cluster):

    def __init__(
        self,
        cores=1,
        processes=1,
        walltime="3:59",
        death_timeout="600s",
        **kwargs
    ):

        # set environment variables for maximum multithreading
        tpw = 2*cores  # threads per worker
        env_extra = [
            f"export MKL_NUM_THREADS={tpw}",
            f"export NUM_MKL_THREADS={tpw}",
            f"export OPENBLAS_NUM_THREADS={tpw}",
            f"export OPENMP_NUM_THREADS={tpw}",
            f"export OMP_NUM_THREADS={tpw}",
        ]

        # get/set directories of interest
        USER = os.environ["USER"]
        CWD = os.getcwd()
        PID = os.getpid()
        if "local_directory" not in kwargs:
            kwargs["local_directory"] = f"/scratch/{USER}/"
        if "log_directory" not in kwargs:
            log_dir = f"{CWD}/dask_worker_logs_{PID}/"
            Path(log_dir).mkdir(parents=False, exist_ok=True)
            kwargs["log_directory"] = log_dir

        # set all core/memory related variables
        memory = str(15*cores)+'GB'
        ncpus = cores
        mem = int(15e9*cores)

        # create cluster
        cluster = LSFCluster(
            cores=cores,
            ncpus=ncpus,
            memory=memory,
            mem=mem,
            processes=processes,
            walltime=walltime,
            death_timeout=death_timeout,
            env_extra=env_extra,
            **kwargs,
        )
        client = Client(cluster)
        self.set_cluster(cluster)
        self.set_client(client)
        print("Cluster dashboard link: ", cluster.dashboard_link)
        sys.stdout.flush()


class local_cluster(_cluster):

    def __init__(self, **kwargs):
        if "host" not in kwargs:
            kwargs["host"] = ""
        cluster = LocalCluster(**kwargs)
        self.set_cluster(cluster)

