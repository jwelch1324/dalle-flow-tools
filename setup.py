from setuptools import find_packages, setup


setup(
    name='dalle_sessions',
    version='0.0.1',
        packages=find_packages('src'),
        package_dir={'': 'src'},
    long_description=open('README.md').read(),
    install_requires=[
        "numpy",
        "pandas",
        "tqdm",
        "python-dateutil"
        ],
)
