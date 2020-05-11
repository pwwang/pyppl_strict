# -*- coding: utf-8 -*-

# DO NOT EDIT THIS FILE!
# This file has been autogenerated by dephell <3
# https://github.com/dephell/dephell

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = ''

setup(
    long_description=readme,
    name='pyppl_strict',
    version='0.0.6',
    description='More strict check of job success for PyPPL',
    python_requires='==3.*,>=3.6.0',
    author='pwwang',
    author_email='pwwang@pwwang.com',
    license='MIT',
    entry_points={"pyppl": ["pyppl_strict = pyppl_strict"]},
    packages=[],
    package_dir={"": "."},
    package_data={},
    install_requires=['pyppl'],
    extras_require={"dev": ["pytest", "pytest-cov"]},
)
