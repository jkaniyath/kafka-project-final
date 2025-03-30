from setuptools import setup, find_packages

import sys
sys.path.append('./src')

from datetime import datetime, timezone
import python_package

setup(
    name="kafka-databricks-stream",
    version=python_package.__version__ + "+" + datetime.now(timezone.utc).strftime("%Y%m%d.%H%M%S"),
    author="jaseemali2@gmail.com",
    description="wheel file for databricks-kafka-stream project",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    install_requires=[
        "setuptools",  
        "databricks-connect==15.1.*" ,
        "chispa==0.10.1",
        "pytest"
    ],
)