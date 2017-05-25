from setuptools import setup, find_packages
setup(
    name="linkapp.link",
    version="0.1",
    packages=["linkapp.link"],
    install_requires=['redis', 'pika', 'strict_rfc3339', 'jsonschema', 'webob', 'requests']
)