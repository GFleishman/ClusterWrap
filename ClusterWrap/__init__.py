from shutil import which
import os
from .clusters import janelia_lsf_cluster, local_cluster

cluster = local_cluster

if which('bsub') is not None:
    test_version = os.system('bsub -V > /dev/null 2>&1')
    # Check if version call was succesful (32512: fail, 512: Unknown option).
    if (test_version != 32512) | (test_version != 512):
        cluster = janelia_lsf_cluster