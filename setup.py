#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = [
    'pandas>=0.21.0',
    'numpy>=1.13.0',
    'python_dateutil>=2.6.0',
    'scipy>=1.0.0'
    ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="MIT Data to AI Lab",
    author_email='dai-lab-trane@mit.edu',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Trane is a software package for automatically generating prediction problems and generating labels for supervised learning.",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='trane',
    name='trane',
    packages=find_packages(include=['trane', 'trane.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/HDI-Project/Trane',
    version='0.1.0',
    zip_safe=False,
)
