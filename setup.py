import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read('README.md') if os.path.isfile("README.md") else ""

setup(
    name='bigquery-to-pubsub',
    version='0.0.1',
    author='Evgeny Medvedev',
    author_email='evge.medvedev@gmail.com',
    description='A tool for streaming time series data from a BigQuery table to a Pub/Sub topic',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/blockchain-etl/bigquery-to-pubsub',
    packages=find_packages(exclude=['schemas', 'tests']),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords=['bigquery', 'pubsub', 'gcp'],
    python_requires='>=3.5.3,<3.8.0',
    install_requires=[
        'click==7.0',
        'google-cloud-pubsub==1.0.2',
        'google-cloud-storage==1.20.0',
        'google-cloud-bigquery==1.21.0',
        'timeout-decorator==0.4.1',
        'python-dateutil==2.7.0',
    ],
    extras_require={
        'dev': [
            'pytest~=4.3.0'
        ]
    },
    entry_points={
        'console_scripts': [
            'replay_bigquery_to_pubsub=bigquery_to_pubsub.cli:replay_bigquery_to_pubsub.py',
        ],
    },
    project_urls={
        'Bug Reports': 'https://github.com/blockchain-etl/bigquery-to-pubsub/issues',
        'Source': 'https://github.com/blockchain-etl/bigquery-to-pubsub',
    },
)
