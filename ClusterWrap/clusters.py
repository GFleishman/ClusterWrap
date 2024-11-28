from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster, SLURMCluster
from dask_jobqueue.lsf import LSFJob
import dask.config
from pathlib import Path
import os
import sys
import time
import yaml



clusters = {
    'local_cluster':local_cluster,
    'remote_cluster':remote_cluster,
    'janelia_lsf_cluster':janelia_lsf_cluster,,
    'lancaster_slurm_cluster':lancaster_slurm_cluster,
}


class _cluster(object):

    def __init__(self, persist_yaml=False, yaml_path=None):
        self.client = None
        self.cluster = None
        self.persist_yaml = persist_yaml
        self.yaml_path = yaml_path

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


class lancaster_slurm_cluster(_cluster):
    """
    A dask cluster configured to run on the Lancaster Slurm Cluster (HEC)

    Parameters
    ----------
    ncpus : int (default: 4)
        The number of cpus that each slurm worker should have.

    processes : int (default: 1)
        The number of dask workers that should spawn on each slurm worker.
        A slurm worker is a set of compute resources allocated by slurm
        running some number of python processes. Each such python process
        is a dask worker. By increasing this parameter you increase the
        number of dask workers running within a single slurm worker.

    threads : int (default: None)
        The number of compute threads dask is allowed to spawn on each
        dask worker. Often if you are using numpy or other highly
        multithreaded code, you do not want dask itself to spawn
        many additional compute threads. In that case, dask should have
        one or two compute threads per worker and any additional cpu cores
        on the worker will be utilized by the underlying multithreaded libraries.

    min_workers : int (default: 1)
        The minimum number of dask workers the cluster will have at any
        given time.

    max_workers : int (default 4)
        The maximum number of dask workers the cluster will have at any
        given time. The cluster will dynamically adjust the number of
        workers between `min_workers` and `max_workers` based on the
        number of tasks submitted by the scheduler.

    walltime : string (default: "3:59")
        The maximum lifetime of each slurm worker (which comprises one
        or more dask workers). The default is chosen so that slurm jobs
        will be submitted to the serial queue by default, and could thus
        be run on any available cluster hardware.

    config : dict (default: {})
        Any additional arguments to configure dask, the distributed
        scheduler, the nanny etc. See:
        https://docs.dask.org/en/latest/configuration.html

    **kwargs : any additional keyword argument
        Additional arguments passed to dask_jobqueue.SLURMCluster.
        Noteable arguments include:
        death_timeout, project, queue, env_extra
        See:
        https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
        for a more complete list.

    Returns
    -------
    A configured lancaster_slurm_cluster object ready to receive tasks
    from a dask scheduler
    """

    def __init__(
        self,
        cores=1,
        memory='1GB',
        processes=1,
        min_workers=1,
        max_workers=4,
        config={},
        **kwargs
    ):

        # call super constructor
        super().__init__()

        # set config defaults
        # comm.timeouts values are needed for scaling up big clusters
        USER = os.environ["USER"]
        SCRATCH = os.environ["global_scratch"]
        scratch_dir = f"/{SCRATCH}/{USER}/"
        Path(scratch_dir).mkdir(parents=False, exist_ok=True)
        config_defaults = {
            'temporary-directory':scratch_dir
            'distributed.comm.timeouts.connect':'180s',
            'distributed.comm.timeouts.tcp':'360s',
        }
        config_defaults = {**config_defaults, **config}
        self.modify_dask_config(config_defaults)

        # store ncpus per worker and worker limits
        self.adapt = None
        self.cores = cores
        self.memory = memory
        self.min_workers = min_workers
        self.max_workers = max_workers

        # set environment vars
        # prevent overthreading outside dask
        tpw = 2*cores  # threads per worker
        job_script_prologue = [
            f"export MKL_NUM_THREADS={tpw}",
            f"export NUM_MKL_THREADS={tpw}",
            f"export OPENBLAS_NUM_THREADS={tpw}",
            f"export OPENMP_NUM_THREADS={tpw}",
            f"export OMP_NUM_THREADS={tpw}",
        ]

        # set local and log directories
        CWD = os.getcwd()
        PID = os.getpid()
        if "local_directory" not in kwargs:
            kwargs["local_directory"] = scratch_dir
        if "log_directory" not in kwargs:
            log_dir = f"{CWD}/dask_worker_logs_{PID}/"
            Path(log_dir).mkdir(parents=False, exist_ok=True)
            kwargs["log_directory"] = log_dir

        # create cluster
        cluster = SLURMCluster(
            cores=cores,
            memory=memory,
            processes=processes,
            job_script_prologue=job_script_prologue,
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


    def change_worker_attributes(
        self,
        min_workers,
        max_workers,
        **kwargs,
    ):
        # TODO: this function is dangerous as written
        #       should not be used by someone who doesn't know
        #       what they're doing
        self.cluster.scale(0)
        for k, v in kwargs.items():
            self.cluster.new_spec['options'][k] = v
        self.adapt_cluster(min_workers, max_workers)


    def adapt_cluster(self, min_workers=None, max_workers=None):

        # store limits
        if min_workers is not None:
            self.min_workers = min_workers
        if max_workers is not None:
            self.max_workers = max_workers
        self.adapt = self.cluster.adapt(
            minimum_jobs=self.min_workers,
            maximum_jobs=self.max_workers,
            interval='10s',
            wait_count=6,
        )

        # give feedback to user
        mn, mx, nc, mm = self.min_workers, self.max_workers, self.cores, self.memory  # shorthand
        print(f"Cluster adapting between {mn} and {mx} workers with {nc} cores and {mm} RAM per worker")


# a class to help exit gracefully on our cluster
class janelia_LSFJob(LSFJob):
    cancel_command = "bkill -d"

class janelia_LSFCluster(LSFCluster):
    job_cls = janelia_LSFJob

class janelia_lsf_cluster(_cluster):
    """
    A dask cluster configured to run on the Janelia LSF compute cluster

    Parameters
    ----------
    ncpus : int (default: 4)
        The number of cpus that each lsf worker should have.
        Of course this also controls total RAM: 15GB per cpu.
        See `processes` parameter for distinction between lsf worker
        and dask worker.

    processes : int (default: 1)
        The number of dask workers that should spawn on each lsf worker.
        An lsf worker is a set of compute resources allocated by lsf
        running some number of python processes. Each such python process
        is a dask worker. By increasing this parameter you increase the
        number of dask workers running within a single lsf worker.

    threads : int (default: None)
        The number of compute threads dask is allowed to spawn on each
        dask worker. Often if you are using numpy or other highly
        multithreaded code, you do not want dask itself to spawn
        many additional compute threads. In that case, dask should have
        one or two compute threads per worker and any additional cpu cores
        on the worker will be utilized by the underlying multithreaded libraries.

    min_workers : int (default: 1)
        The minimum number of dask workers the cluster will have at any
        given time.

    max_workers : int (default 4)
        The maximum number of dask workers the cluster will have at any
        given time. The cluster will dynamically adjust the number of
        workers between `min_workers` and `max_workers` based on the
        number of tasks submitted by the scheduler.

    walltime : string (default: "3:59")
        The maximum lifetime of each lsf worker (which comprises one
        or more dask workers). The default is chosen so that lsf jobs
        will be submitted to the cloud queue by default, and could thus
        be run on any available cluster hardware.

    config : dict (default: {})
        Any additional arguments to configure dask, the distributed
        scheduler, the nanny etc. See:
        https://docs.dask.org/en/latest/configuration.html

    **kwargs : any additional keyword argument
        Additional arguments passed to dask_jobqueue.LSFCluster.
        Noteable arguments include:
        death_timeout, project, queue, env_extra
        See:
        https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.LSFCluster.html
        for a more complete list.

    Returns
    -------
    A configured janelia_lsf_cluster object ready to receive tasks
    from a dask scheduler
    """

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
        USER = os.environ["USER"]
        config_defaults = {
            'temporary-directory':f"/scratch/{USER}/",
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
        job_script_prologue = [
            f"export MKL_NUM_THREADS={tpw}",
            f"export NUM_MKL_THREADS={tpw}",
            f"export OPENBLAS_NUM_THREADS={tpw}",
            f"export OPENMP_NUM_THREADS={tpw}",
            f"export OMP_NUM_THREADS={tpw}",
        ]

        # set local and log directories
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
            job_script_prologue=job_script_prologue,
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


    def change_worker_attributes(
        self,
        min_workers,
        max_workers,
        **kwargs,
    ):
        # TODO: this function is dangerous as written
        #       should not be used by someone who doesn't know
        #       what they're doing
        self.cluster.scale(0)
        for k, v in kwargs.items():
            self.cluster.new_spec['options'][k] = v
        self.adapt_cluster(min_workers, max_workers)


    def adapt_cluster(self, min_workers=None, max_workers=None):

        # store limits
        if min_workers is not None:
            self.min_workers = min_workers
        if max_workers is not None:
            self.max_workers = max_workers
        self.adapt = self.cluster.adapt(
            minimum_jobs=self.min_workers,
            maximum_jobs=self.max_workers,
            interval='10s',
            wait_count=6,
        )

        # give feedback to user
        mn, mx, nc = self.min_workers, self.max_workers, self.ncpus  # shorthand
        cost = round(mx * nc * self.HOURLY_RATE_PER_CORE, 2)
        print(f"Cluster adapting between {mn} and {mx} workers with {nc} cores per worker")
        print(f"*** This cluster has an upper bound cost of {cost} dollars per hour ***")




class local_cluster(_cluster):
    """
    This is a thin wrapper around dask.distributed.LocalCluster
    For a list of full arguments (how to specify your worker resources)
    see:
    https://distributed.dask.org/en/latest/api.html#distributed.LocalCluster

    You need to know how many cpu cores and how much RAM your machine has.
    Most users will only need to specify:
    n_workers
    memory_limit (which is the limit per worker)
    threads_per_workers (for most workflows this should be 1)
    """

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


