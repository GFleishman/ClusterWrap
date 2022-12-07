# ClusterWrap
Wrappers around dask-jobqueue cluster objects for specific on-site clusters

## Installation
---
> pip install ClusterWrap

## Notes
---
The ClusterWrap.cluster object tries to determine which cluster you want to use. If the `bsub` command is in your environment ClusterWrap.cluster will default to the `janelia_lsf_cluster` implementation. If `bsub` is not found it will default to a `LocalCluster` which should run on your workstation or laptop. Of course, cluster objects in ClusterWrap.clusters can be constructed directly as well.

Clusters are implemented as context managers. This ensures workers are properly shut down either when distributed computing is complete, or if an uncaught exception (error) is thrown during execution. For `janelia_lsf_cluster` objects, clusters will scale automatically between a minimum and maximum number of workers based on the amount of work being submitted.

Finally, a decorator @cluster is provided to make distributing a function to a cluster very easy.

## Example usage
---
### Decorating a function with a cluster
```python
from ClusterWrap.decorator import cluster

@cluster
def your_distributed_function(..., cluster=None, cluster_kwargs={}):
    """ Your dask code"""
```
Any function decorated with `@cluster` must take the `cluster` and `cluster_kwargs` parameters. When `your_distributed_function` is called, if the `cluster is not None` then the function simply executes on the given cluster. If `cluster is None` then a new cluster is created using the parameters specified in `cluster_kwargs`.

### Making your own cluster
Working on a laptop or workstation:
```python
import ClusterWrap

# Start local cluster
with ClusterWrap.cluster() as cluster:
    """ Your dask code """
```

Working on the janelia cluster
```python
import ClusterWrap

# Start janelia_lsf_cluster that adapts between 1 and 100 workers with 2 cores per worker
cluster_kwargs = {'min_workers':1, 'max_workers':100, 'ncpus':2}
With ClusterWrap.cluster(**cluster_kwargs) as cluster:
    """ Your dask code """
```

For the Janelia cluster, by default workers could be put on either cloud or local nodes (whichever is more available according to LSF) and each worker will run for a maximum of 3 hours and 59 minutes. If you want to force a specific queue you can use the `queue` keyword. For example, if you want to run workers on the short queue (for fast distributed jobs) then you will need: `cluster_kwargs = {'walltime':'1:00', 'queue':'short'}`.

If you need your workers to persist for more than 3 hours and 59 minutes, then you will need to put them in the local queue: `cluster_kwargs = {'walltime':'12:00', 'queue':'local'}`. Workers in this example will persist for a maximum of 12 hours.

Stdout and Stderr output for each worker will be stored in a subdirectory called "dask_worker_logs_{PID}" with PID being the process ID of the python process used to create the cluster. This direcory will be located in current working directory of the python process at the time of cluster creation.

If you need to bill a specific project other than your default group, use: `cluster_kwargs = {'project':'billed_group'}`

