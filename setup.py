"""Setup file for simple-kp package."""
from setuptools import setup

setup(
    name='simple_kp',
    version='2.1.0',
    author='Patrick Wang',
    author_email='patrick@covar.com',
    url='https://github.com/ranking-agent/simple-kp',
    description='Translator KP Registry',
    packages=['simple_kp'],
    include_package_data=True,
    zip_safe=False,
    license='MIT',
    python_requires='>=3.7',
)
