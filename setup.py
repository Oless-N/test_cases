from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='test_cases',
    version='0.0.1',
    author='Oless_N',
    author_email='korklal@gmail.com',
    description='test_cases service',
    install_requires=required,
    scripts=['app/main.py', 'scripts/create_db.py']
)
