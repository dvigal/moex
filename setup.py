from setuptools import setup

setup(
    version='0.0.3',
    name='pymoex',
    description='Unofficial ISS MOEX API on Python',
    url='https://github.com/ntcny/moex',
    author='Anton Parshutin',
    author_email='n.tcnie@gmail.com',
    license='GNU',
    install_requires=['pandas>=0.18.0'],
    packages=['moex'],
    zip_safe=False)
