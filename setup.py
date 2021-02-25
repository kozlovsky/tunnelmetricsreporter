from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='tunnelmetricsreporter',
    author='Alexander Kozlovsky',
    description='Reports anonymized metrics from Tribler exit nodes',
    long_description=long_description,
    long_description_content_type='text/markdown',
    version='0.1',
    url='https://github.com/kozlovsky/tunnelmetricsreporter',
    packages=find_packages(),
    install_requires=[
        "pydantic",
        "requests",
    ],
    tests_require=[
        'pytest'
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
