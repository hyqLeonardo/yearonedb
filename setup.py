from setuptools import setup

setup(name='yearonedb',
      version='1.0.0',
      description='Hoard the mining',
      url='http://github.com/hyqLeonardo/yearonedb',
      author='Leonardo',
      author_email='hyq335335@163.com',
      license='MIT',
      packages=['yearonedb'],
      install_requires=[
          'mysqlclient',
          'sqlalchemy'
      ],
      zip_safe=False)
