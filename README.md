# ClusterWrap
Wrappers around dask-jobqueue functions for specific clusters

## Installation
---
> pip install ClusterWrap

## Usage
---
Clusters are implemented as context managers. This ensures workers are properly shut down either when distributed computing is complete, or if an uncaught exception (error) is thrown during execution. Here is an example for the Janelia cluster:

```python
from ClusterWrap.clusters import janelia_lsf_cluster

with janelia_lsf_cluster() as cluster:
    cluster.scale_cluster(nworkers)
    print(cluster.get_dashboard())
    """ Code that utilizes cluster """

""" The cluster shuts down automatically when you exit the with block """
```

Typically the first thing you'll do inside your `with` block is create workers. Here `nworkers` is an integer specifying the number of workers your cluster is composed of. `cluster.scale_cluster` can be called more than once to dynamically resize your cluster as your distributed needs change.

The printed link will take you to the dask dashboard to monitor the state of your cluster including any jobs executing on it. The dashboard requires bokeh which does not come with ClusterWrap. If the dashboard does not work you may need to run `pip install bokeh`.

By default for the Janelia cluster, each worker has 1 core (and 15GB of RAM). You can change this when you create the cluster with `janelia_lsf_cluster(cores=n)`; in which case each worker will have n cores (and n\*15GB RAM).

For the Janelia cluster, by default workers could be put on either cloud or local nodes (whichever is more available according to LSF) and each worker will run for a maximum of 3 hours and 59 minutes. If you want to force a specific queue you can use the `queue` keyword. For example, if you want to run workers on the short queue (for fast distributed jobs) then you will need: `janelia_lsf_cluster(walltime="1:00", queue="short")`.

If you need your workers to persist for more than 3 hours and 59 minutes, then you will need to put them in the local queue: `janelia_lsf_cluster(walltime="12:00", queue="local")`. Workers in this example will persist for a maximum of 12 hours.

Stdout and Stderr output for each worker will be stored in a subdirectory called "dask_worker_logs_{PID}" with PID being the process ID of the python process used to create the cluster. This direcory will be located in current working directory of the python process at the time of cluster creation.

If you need to bill a specific project other than your default group, use: `janelia_lsf_cluster(project="billed_group")`

