import os
from importlib.machinery import SourceFileLoader

from setuptools import find_packages, setup

module = SourceFileLoader(
    "version", os.path.join("aiohttp_s3_client", "version.py")
).load_module()


setup(
    name="aiohttp-s3-client",
    version=module.__version__,
    author=module.__author__,
    author_email=module.team_email,
    license=module.package_license,
    description=module.package_info,
    long_description_content_type='text/markdown',
    long_description=open("README.md").read(),
    platforms="all",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Internet",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "aiomisc~=14.4", "aiohttp<4", "aws-request-signer==1.0.0",
    ],
    python_requires=">3.7.*, <4",
    extras_require={
        "develop": [
            "coverage!=4.3",
            "coveralls",
            "mypy",
            "pylava",
            "pytest",
            "pytest-aiohttp",
            "pytest-cov",
            "tox>=2.4",
        ],
    },
    project_urls={
        "Source": "https://github.com/mosquito/aiohttp-s3-client",
    },
)
