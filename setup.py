from setuptools import setup

from adruk_tools._version import __version__


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='adruk_tools',
    version=__version__,
    description='collection of pyspark functions written by the ADRUK team',
    url='https://gitlab-app-l-01/adr_eng/adruk_tools.git',
    author='Veronica Ferreiros Lopez, Johannes Hechler, Nathan Shaw, Silvia Bardoni',
    packages=['adruk_tools'],
    install_requires=requirements
)
