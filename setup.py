#!/usr/bin/env python

from setuptools import setup

setup(
    name='sparkstreaming',
    version='1.0.0',
    description='BDCC Pyspark Streaming project',
    py_moudles=['__main__'],
    packages=['src.main.python'],
    zip_safe=False
)
