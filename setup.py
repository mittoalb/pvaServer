from setuptools import setup, find_packages, find_namespace_packages

setup(
    name='pvaserver',
    version=open('VERSION').read().strip(),
    #version=__version__,
    author='Francesco De Carlo',
    author_email='decarlof@gmail.com',
    url='https://github.com/decarlof/pvaserver',
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    entry_points = {
        'console_scripts': ['pvaserver=pvaserver.__main__:main'],
    },
    description='pva stream generator',
    zip_safe=False,
)
