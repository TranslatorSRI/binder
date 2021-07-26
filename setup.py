"""Setup file for simple-kp package."""
from setuptools import setup

setup(
    name='simple_kp',
    version='3.0.0',
    author='Patrick Wang',
    author_email='patrick@covar.com',
    url='https://github.com/ranking-agent/simple-kp',
    description='Translator KP Registry',
    packages=['simple_kp'],
    include_package_data=True,
    install_requires=[
        "aiosqlite>=0.16.0",
        "bmt-lite-1.8.2>=1.0.2,<2.0",
        "fastapi>=0.65.2",
        "reasoner-pydantic>=1.1.2.1,<1.1.3",
    ],
    zip_safe=False,
    license='MIT',
    python_requires='>=3.6',
)
