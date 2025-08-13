from setuptools import setup, find_packages

setup(
    name="bilob",
    version="0.1.0",
    author="Gyeongjun",
    author_email="cka09191@gmail.com",
    description="Simple binance autotrading system with pretrained prediction model",
    packages=find_packages(exclude=["tests*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "torch", "numpy"
    ],
    extras_require={
        "dev": ["pytest", "pytest-cov"],
    },
    entry_points={
        'console_scripts': [
            'gridbid=bilob.utils.grid:bid',
            'gridask=bilob.utils.grid:ask',
            'orderbrief=bilob.utils.overview:overview',
        ],
    },
    package_data={
        'bilob.utils': ['config.ini'],
    },
)

