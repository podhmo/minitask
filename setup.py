from setuptools import setup, find_packages


setup(
    classifiers=[
        # "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
    ],
    python_requires=">3.5",
    packages=find_packages(exclude=["minitask.tests"]),
    install_requires=["handofcats", "magicalimport"],
    extras_require={
        "testing": ["pytest"],
        "dev": ["black", "flake8", "mypy"],
        "sqs": ["boto3"],
    },
    tests_require=["pytest"],
    test_suite="minitask.tests",
    #     entry_points="""
    #       [console_scripts]
    #       minitask = minitask.cli:main
    # """,
)
