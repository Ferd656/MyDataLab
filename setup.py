from setuptools import setup, find_packages

setup(
    name="mydatalab",
    version="0.0.1",
    description="Utility panel with graphical interface",
    author="Ferd",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy"
    ],
    python_requires=">=3.7",
)