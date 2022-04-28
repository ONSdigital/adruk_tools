from setuptools import setup

from adruk_tools._version import __version__


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='adruk_tools',
    version=__version__,
    description='collection of pyspark functions written by the ADRUK team',
    url='http://np2rvlapxx507/data_engineering/de-utils.git',
    author='Sophie-Louise Courtney, Johannes Hechler, Nathan Shaw, Silvia Bardoni',
    packages=['adruk_tools'],
    install_requires=requirements
)
