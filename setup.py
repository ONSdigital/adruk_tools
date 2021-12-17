from setuptools import setup

from adruk_tools._version import __version__


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='adruk_tools',
    version=__version__,
    description='collection of pyspark functions written by the ADRUK team for data engineering',
    url='http://np2rvlapxx507/data_engineering/de-utils.git',
    author = 'Sophie-Louise Courtney, Johannes Hechler, Ben Marshall-Sheen, Thomas Heneghan',
    packages = ['adruk_tools','adruk_tools.functions'],
    install_requires=requirements
)
