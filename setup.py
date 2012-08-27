from setuptools import setup

setup(
    name='Dashgourd-Import-Mysql',
    version='0.2.6',
    url='https://github.com/richard-to/dashgourd-import-mysql',
    author='Richard To',
    description='Import data from MySQL into Dashgourd',
    platforms='any',
    packages=[
        'dashgourd', 
        'dashgourd.importer',
    ],
    namespace_packages=['dashgourd'],    
    include_package_data=True,
    install_requires=[
        'MySQL-python'
    ]    
)
