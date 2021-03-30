from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import dask.config
from pathlib import Path
import os
import sys
import time

HOURLY_RATE_PER_CORE = 0.07


class _cluster(object):

    def __init__(self):
        self.client = None
        self.cluster = None
        self.max_workers = None
        self.scale_delay = 30
        self.cores = 0

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

        # if limited
        if self.max_workers is not None:
            nworkers = min(self.max_workers, nworkers)

        # scale
        self.cluster.scale(jobs=nworkers)

        # give feedback and give cluster some time to scale
        cost = round(nworkers * self.cores * HOURLY_RATE_PER_CORE, 2)
        print(f"Scaling cluster to {nworkers} workers with {self.cores} cores per worker")
        print(f"*** This cluster costs {cost} dollars per hour starting now ***")
        print(f"Waiting {self.scale_delay} seconds for cluster to scale")
        time.sleep(self.scale_delay)
        print("Wait time complete")

    def modify_dask_config(self, options):
        dask.config.set(options)

    def get_dashboard(self):
        if self.cluster is not None:
            return self.cluster.dashboard_link

    def set_max_workers(self, max_workers):
        self.max_workers = max_workers

    def set_scale_delay(self, scale_delay):
        self.scale_delay = scale_delay


class janelia_lsf_cluster(_cluster):

    def __init__(
        self,
        cores=1,
        walltime="3:59",
        max_workers=None,
        scale_delay=None,
        config=None,
        **kwargs
    ):

        # call super constructor
        super().__init__()

        # modify config
        if config is not None:
            self.modify_dask_config(config)

        # store cores/per worker
        self.cores = cores

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

        # set max workers
        if max_workers is not None:
            self.set_max_workers(max_workers)

        # set scale delay
        if scale_delay is not None:
            self.scale_delay = scale_delay

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
            walltime=walltime,
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

