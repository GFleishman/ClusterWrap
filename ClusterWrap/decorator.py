import functools
from ClusterWrap import cluster as cluster_constructor


def cluster(func):
    """
    """

    # define wrapper
    @functools.wraps(func)
    def create_or_pass_cluster(*args, **kwargs):

        # determine current state of cluster
        cluster = kwargs['cluster'] if 'cluster' in kwargs else None

        # if there is no cluster, make one
        if cluster is None:

            # get arguments and run function in cluster context
            x = kwargs['cluster_kwargs'] if 'cluster_kwargs' in kwargs else {}
            with cluster_constructor(**x) as cluster:
                kwargs['cluster'] = cluster
                return func(*args, **kwargs)

        # otherwise, there is already a cluster so just run the function
        return func(*args, **kwargs)

    # return decorated function
    return create_or_pass_cluster

