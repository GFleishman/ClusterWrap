import functools
from ClusterWrap.clusters import clusters

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

            # get cluster type
            assert ('cluster_type' in kwargs['cluster_kwargs'].keys()),
                "cluster_type must be defined in cluster_kwargs"
            cluster_constructor = clusters[kwargs['cluster_kwargs']['cluster_type']]
            del kwargs['cluster_kwargs']['cluster_type']

            # get arguments and run function in cluster context
            x = kwargs['cluster_kwargs'] if 'cluster_kwargs' in kwargs else {}
            with cluster_constructor(**x) as cluster:
                kwargs['cluster'] = cluster
                return func(*args, **kwargs)

        # otherwise, there is already a cluster so just run the function
        return func(*args, **kwargs)

    # return decorated function
    return create_or_pass_cluster

