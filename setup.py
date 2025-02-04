import setuptools

setuptools.setup(
    name="ClusterWrap",
    version="1.0.2",
    author="Greg M. Fleishman",
    author_email="greg.nli10me@gmail.com",
    description="Local machine and specific HPC cluster implementations",
    url="https://github.com/GFleishman/ClusterWrap",
    license="MIT",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'dask>=2022.9.1',
        'dask[distributed]>=2022.9.1',
        'dask-jobqueue>=0.7.3',
    ]
)

