# ClusterWrap
Wrappers around dask-jobqueue functions for specific clusters

## Installation
---
> pip install ClusterWrap

## Usage
---
ClusterWrap will automatically look for the `bsub` command in your environment. If found it will default to the `janelia_lsf_cluster`, if not found it will default to a `LocalCluster` which should run on your workstation or laptop. Clusters are implemented as context managers. This ensures workers are properly shut down either when distributed computing is complete, or if an uncaught exception (error) is thrown during execution. For `janelia_lsf_cluster` objects, clusters will scale automatically between a minimum and maximum number of workers based on the amount of work being submitted.

```python
import ClusterWrap

# Start local cluster
with ClusterWrap.cluster() as cluster:
    """ Code that utilizes local cluster """



# Start janelia_lsf_cluster that adapts between 1 and 100 workers with 2 cores per worker
cluster_kwargs = {'min_workers':1, 'max_workers':100, 'cores':2}
With ClusterWrap.cluster(**cluster_kwargs) as cluster:
    """ Code that utilizes janelia cluster """

    # change adaptive scaling bounds
    cluster.adapt_cluster(10, 200)
    """ Code that utilizes increased number of workers """



""" The cluster shuts down automatically when you exit the with block """
```

For the Janelia cluster, by default workers could be put on either cloud or local nodes (whichever is more available according to LSF) and each worker will run for a maximum of 3 hours and 59 minutes. If you want to force a specific queue you can use the `queue` keyword. For example, if you want to run workers on the short queue (for fast distributed jobs) then you will need: `cluster_kwargs = {'walltime':'1:00', 'queue':'short'}`.

If you need your workers to persist for more than 3 hours and 59 minutes, then you will need to put them in the local queue: `cluster_kwargs = {'walltime':'12:00', 'queue':'local'}`. Workers in this example will persist for a maximum of 12 hours.

Stdout and Stderr output for each worker will be stored in a subdirectory called "dask_worker_logs_{PID}" with PID being the process ID of the python process used to create the cluster. This direcory will be located in current working directory of the python process at the time of cluster creation.

If you need to bill a specific project other than your default group, use: `cluster_kwargs = {'project':'billed_group'}`

