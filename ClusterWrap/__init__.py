from shutil import which
import os
from .clusters import janelia_lsf_cluster, local_cluster

cluster = local_cluster

if which('bsub') is not None:
    if os.system('bsub -V') != 32512:
        cluster = janelia_lsf_cluster


