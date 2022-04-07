from setuptools import setup, find_packages

setup(
    name="didomi_spark",
    version="1.0.0",
    description="Data challenge for Didomi",
    url="https://github.com/gaarv/didomi-data-challenge",
    author="Sebastien Hoarau",
    author_email="sebastien.h.data.eng@gmail.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    packages=find_packages(),
    python_requires=">=3.8, <4",
    extras_require={
        "dev": ["pip-tools<=6.6.0", "black", "pytest<=7.1.1"],
        "test": ["coverage", "pytest<=7.1.1"],
    },
    package_data={
        "didomi_spark": ["data/input.zip"],
    },
)
