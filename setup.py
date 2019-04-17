from os import path
from setuptools import setup, find_packages


here = path.abspath(path.dirname(__file__))

# Get the version number without loading dependencies 
with open(path.join(here,'ch_dataset','version.py')) as f:
    for line in f:
        if line.startswith('__version__'):
            exec(line)
            break
    else:
    	raise RuntimeError('Cannot find version number')

# Load the requirements from requirements.txt while removing the environment marks
with open(path.join(here, 'requirements.txt')) as f:
    requirements = [line.split(';')[0].rstrip() for line in f]

setup(
    name='ch_dataset',
    version=__version__, #wtl.metrics.__version__,
    description="CHIME dataset server",
    url='https://bitbucket.org/chime/ch_dataset',
    author='Rick Nitsche',
    author_email='rick@phas.ubc.ca',
    packages=find_packages(),
    install_requires= requirements,
    include_package_data=True,

)
