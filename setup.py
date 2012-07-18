from distutils.core import setup
import sys

setup(name = "SpecClient",version = "1.0",
      description = "Python package for communicating with spec", 
      author="Matias Guijarro, BCU(Bliss), ESRF",
      package_dir={"SpecClient_gevent": "SpecClient"},
      packages = ["SpecClient_gevent"])

