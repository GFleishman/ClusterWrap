from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from dask_jobqueue.lsf import LSFJob
import dask.config
from pathlib import Path
import os
import sys
import time
import yaml


# a class to help exit gracefully on our cluster
class janelia_LSFJob(LSFJob):
    cancel_command = "bkill -d"

class janelia_LSFCluster(LSFCluster):
    job_cls = janelia_LSFJob


class _cluster(object):

    def __init__(self):
        self.client = None
        self.cluster = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if not self.persist_yaml:
            if os.path.exists(self.yaml_path):
                os.remove(self.yaml_path)
        self.client.close()
        self.cluster.__exit__(exc_type, exc_value, traceback)

    def set_client(self, client):
        self.client = client
    def set_cluster(self, cluster):
        self.cluster = cluster


    def modify_dask_config(
        self, options, yaml_name='ClusterWrap.yaml', persist_yaml=False,
    ):
        dask.config.set(options)
        yaml_path = str(Path.home()) + '/.config/dask/' + yaml_name
        with open(yaml_path, 'w') as f:
            yaml.dump(dask.config.config, f, default_flow_style=False)
        self.yaml_path = yaml_path
        self.persist_yaml = persist_yaml


    def get_dashboard(self):
        if self.cluster is not None:
            return self.cluster.dashboard_link

# TODO: not needed?
#    def adapt_cluster(self, min_workers=None, max_workers=None):
#        None



class janelia_lsf_cluster(_cluster):

    HOURLY_RATE_PER_CORE = 0.07

    def __init__(
        self,
        ncpus=4,
        processes=1,
        threads=None,
        min_workers=1,
        max_workers=4,
        walltime="3:59",
        config={},
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
        config_defaults = {**config_defaults, **config}
        self.modify_dask_config(config_defaults)

        # store ncpus/per worker and worker limits
        self.adapt = None
        self.ncpus = ncpus
        self.min_workers = min_workers
        self.max_workers = max_workers

        # set environment vars
        # prevent overthreading outside dask
        tpw = 2*ncpus  # threads per worker
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

        # compute ncpus/RAM relationship
        memory = str(15*ncpus)+'GB'
        mem = int(15e9*ncpus)

        # determine nthreads
        if threads is None:
            threads = ncpus

        # create cluster
        cluster = janelia_LSFCluster(
            ncpus=ncpus,
            processes=processes,
            memory=memory,
            mem=mem,
            walltime=walltime,
            cores=threads,
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


    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)
        

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
        mn, mx, nc = self.min_workers, self.max_workers, self.ncpus  # shorthand
        cost = round(mx * nc * self.HOURLY_RATE_PER_CORE, 2)
        print(f"Cluster adapting between {mn} and {mx} workers with {nc} cores per worker")
        print(f"*** This cluster has an upper bound cost of {cost} dollars per hour ***")




class local_cluster(_cluster):

    def __init__(
        self,
        config={},
        memory_limit=None,
        **kwargs,
    ):

        # initialize base class
        super().__init__()

        # set config defaults
        config_defaults = {}
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




class remote_cluster(_cluster):

    def __init__(
        self,
        cluster,  # a dask cluster object, could also be IP address, Cristian what do you prefer?
        config={},
    ):

        # initialize base class
        super().__init__()

        # set config defaults
        config_defaults = {}
        config = {**config_defaults, **config}
        self.modify_dask_config(config)

        # setup client
        client = Client(cluster)
        self.set_cluster(cluster)
        self.set_client(client)


