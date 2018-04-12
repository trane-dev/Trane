from setuptools import setup, find_packages


test_requirements = ['pytest', ]


setup(name='trane',
      version='0.0',
      description='Trane is a software package for automatically generating prediction problems and generating labels for supervised learning.',
      url='https://github.com/HDI-Project/Trane',
      packages=find_packages(),
      license='MIT License',
      include_package_data = True,
      author='MIT Data to AI Lab',
      author_email = 'dailabmit@gmail.com',
      tests_require = test_requirements,
      classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    	]

      )
