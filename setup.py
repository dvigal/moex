from setuptools import setup

setup(
    version='0.0.2',
    name='pymoex',
    description='Unofficial ISS MOEX API on Python',
    url='https://github.com/dvigal/moex',
    author='Alexander Litvinov',
    author_email='dvigal@yandex.ru',
    license='GNU',
    install_requires=['pandas>=0.18.0'],
    packages=['moex'],
    zip_safe=False)
