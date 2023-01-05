from setuptools import setup

setup(
    name='PyTripalSerializer',
    version='0.0.1',
    packages=['tripser',
             ],
    license='MIT',
    description="Serialize Tripal's JSON-LD API into RDF format",
    install_requires=['rdflib',
                      'click',
                      'requests'],
)
