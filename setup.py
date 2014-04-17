#from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='datafeed',
    version='0.6',
    author='yinhm',
    author_email='epaulin@gmail.com',    
    packages=['datafeed', 'datafeed/providers', ],
    license='Apache 2.0 Licence',
    long_description=open('README.md').read(),
)
