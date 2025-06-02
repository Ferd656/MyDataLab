from setuptools import setup, find_packages

setup(
    name="mydatalab",
    version="0.0.1",
    description="Utility panel with graphical interface",
    author="Ferd",
    packages=find_packages(),
    install_requires=[
        "pandas"
    ],
    python_requires=">=3.7",
)