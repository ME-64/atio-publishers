from setuptools import setup
from setuptools import find_packages
from glob import glob
import pathlib
import os

# python3 setup.py sdist bdist_wheel
# twine upload dist/*

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.rst").read_text()

exec(open('src/atio_publishers/_version.py').read())


setup(
        name = 'atio-publishers',
        packages = find_packages('src'),
        package_dir = {'': 'src'},
        py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob('src/*.py')],
        version = __version__,
        license='MIT',
        description = 'Utility classes that aid streaming data to the atio trading engine',
        long_description = README,
        long_description_content_type = 'text/x-rst',
        author = 'ME-64',
        author_email = 'milo_elliott@icloud.com',
        url = 'https://github.com/ME-64/atio-publishers',
        keywords = ['redis', 'websockets', 'trading', 'async', 'multiprocessing'],
        include_package_data = True,
        zip_safe = False,
        install_requires=['aiohttp>3.8,<4' ,'pandas>1.4,<2', 'sortedcontainers>2.4<3', 'ujson>=4.3,<5',
            'aioprocessing>=2,<3', 'redis>4.2,<5'],
        extras_require={
            "dev": ['pytest>7,<8', 'pyteset-asyncio>0.17,<1']},
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Intended Audience :: Traders',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3.10',
            ]
        )
