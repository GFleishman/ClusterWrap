from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
import dask.config
from pathlib import Path
import os
import sys
import time
import yaml


class _cluster(object):

    def __init__(self):
        self.client = None
        self.cluster = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.client is not None:
            self.client.shutdown()
            self.client.close()
        if self.cluster is not None:
            self.cluster.close()


    def set_client(self, client):
        self.client = client
    def set_cluster(self, cluster):
        self.cluster = cluster


    def modify_dask_config(
        self, options, yaml_name='ClusterWrap.yaml',
    ):
        dask.config.set(options)
        yaml_path = str(Path.home()) + '/.config/dask/' + yaml_name
        with open(yaml_path, 'w') as f:
            yaml.dump(dask.config.config, f, default_flow_style=False)


    def get_dashboard(self):
        if self.cluster is not None:
            return self.cluster.dashboard_link

    def adapt_cluster(self, min_workers=None, max_workers=None):
        None




class janelia_lsf_cluster(_cluster):

    HOURLY_RATE_PER_CORE = 0.07

    def __init__(
        self,
        cores=4,
        processes=1,
        min_workers=1,
        max_workers=4,
        walltime="3:59",
        config=None,
        **kwargs
    ):

        # call super constructor
        super().__init__()

        # set config defaults
        # comm.timeouts values are needed for scaling up big clusters
        config_defaults = {
            'distributed.comm.timeouts.connect':'180s',
            'distributed.comm.timeouts.tcp':'360s',
        }
        if config is not None:
            config = {**config_defaults, **config}
        self.modify_dask_config(config)

        # store cores/per worker and worker limits
        self.adapt = None
        self.cores = cores
        self.min_workers = min_workers
        self.max_workers = max_workers

        # set environment vars
        # prevent overthreading outside dask
        tpw = 2*cores  # threads per worker
        env_extra = [
            f"export MKL_NUM_THREADS={tpw}",
            f"export NUM_MKL_THREADS={tpw}",
            f"export OPENBLAS_NUM_THREADS={tpw}",
            f"export OPENMP_NUM_THREADS={tpw}",
            f"export OMP_NUM_THREADS={tpw}",
        ]

        # set local and log directories
        USER = os.environ["USER"]
        CWD = os.getcwd()
        PID = os.getpid()
        if "local_directory" not in kwargs:
            kwargs["local_directory"] = f"/scratch/{USER}/"
        if "log_directory" not in kwargs:
            log_dir = f"{CWD}/dask_worker_logs_{PID}/"
            Path(log_dir).mkdir(parents=False, exist_ok=True)
            kwargs["log_directory"] = log_dir

        # compute cores/RAM relationship
        memory = str(15*cores)+'GB'
        ncpus = cores
        mem = int(15e9*cores)

        # create cluster
        cluster = LSFCluster(
            cores=cores,
            processes=processes,
            ncpus=ncpus,
            memory=memory,
            mem=mem,
            walltime=walltime,
            env_extra=env_extra,
            **kwargs,
        )

        # connect cluster to client
        client = Client(cluster)
        self.set_cluster(cluster)
        self.set_client(client)
        print("Cluster dashboard link: ", cluster.dashboard_link)
        sys.stdout.flush()

        # set adaptive cluster bounds
        self.adapt_cluster(min_workers, max_workers)


    def __exit__(self, type, value, traceback):
        if self.adapt is not None:
            self.adapt.stop()
        super().__exit__(type, value, traceback)
        

    def adapt_cluster(self, min_workers=None, max_workers=None):

        # store limits
        if min_workers is not None:
            self.min_workers = min_workers
        if max_workers is not None:
            self.max_workers = max_workers
        self.adapt = self.cluster.adapt(
            minimum_jobs=self.min_workers,
            maximum_jobs=self.max_workers,
        )

        # give feedback to user
        mn, mx, cr = self.min_workers, self.max_workers, self.cores  # shorthand
        cost = round(mx * cr * self.HOURLY_RATE_PER_CORE, 2)
        print(f"Cluster adapting between {mn} and {mx} workers with {cr} cores per worker")
        print(f"*** This cluster has an upper bound cost of {cost} dollars per hour ***")




class local_cluster(_cluster):

    def __init__(
        self,
        config=None,
        memory_limit=None,
        **kwargs,
    ):

        # initialize base class
        super().__init__()

        # set config defaults
        # comm.timeouts values are needed for scaling up big clusters
        config_defaults = {}
        if config is not None:
            config = {**config_defaults, **config}
        self.modify_dask_config(config)

        # set LocalCluster defaults
        if "host" not in kwargs:
            kwargs["host"] = ""
        if memory_limit is not None:
            kwargs["memory_limit"] = memory_limit

        # set up cluster, connect scheduler/client
        cluster = LocalCluster(**kwargs)
        client = Client(cluster)
        self.set_cluster(cluster)
        self.set_client(client)

