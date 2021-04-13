import setuptools

setuptools.setup(
    name="ClusterWrap",
    version="0.1.2",
    author="Greg M. Fleishman",
    author_email="greg.nli10me@gmail.com",
    description="Wrappers around dask-jobqueue functions for specific clusters",
    url="https://github.com/GFleishman/ClusterWrap",
    license="MIT",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'dask',
        'dask[distributed]',
        'dask-jobqueue',
    ]
)

