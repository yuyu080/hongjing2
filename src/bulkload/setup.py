import codecs
import os

from setuptools import setup, find_packages


def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()



PACKAGE = "bbdSDK"
NAME = "bbdSDK"
DESCRIPTION = "bbdSDK"
AUTHOR = "bbd"
AUTHOR_EMAIL = "bbd@bbd.com"
URL = "https://github.com"
VERSION = __import__(PACKAGE).__version__


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=read("README.md"),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(exclude=["tests.*", "tests"],include=['*.md','*.*']),
    zip_safe=True,
)

