from distutils.core import setup

setup(
    name='dagline',
    author='Martin Privat',
    version='0.5.16',
    packages=['dagline','dagline.tests'],
    license='MIT',
    description='DAG multiprocessing pipeline',
    long_description=open('README.md').read(),
    install_requires=[
        "pandas",
        "seaborn",
        "multiprocessing_logger @ git+https://github.com/ElTinmar/multiprocessing_logger.git@v0.3.13",
        "ipc_tools @ git+https://github.com/ElTinmar/ipc_tools.git@v0.4.8"
    ]
)
