"""Setup binder package."""
from setuptools import setup

setup(
    name="binder",
    version="4.0.0",
    author="Patrick Wang",
    author_email="patrick@covar.com",
    url="https://github.com/TranslatorSRI/binder",
    description="Translator *graph binder",
    packages=["binder"],
    include_package_data=True,
    install_requires=[
        "aiosqlite>=0.16.0",
        "bmt-lite-1.8.2>=1.0.2,<2.0",
        "fastapi>=0.65.2",
        "reasoner-pydantic>=1.1.2.1,<1.1.3",
    ],
    zip_safe=False,
    license="MIT",
    python_requires=">=3.6",
)
