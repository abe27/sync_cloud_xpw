from setuptools import setup, find_packages

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name="yazaki_packages",
    version="0.1-beta",
    author="kanomthai",
    author_email="krumii.it@gmail.com",
    description="Some description",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kanomthai/sync_cloud_xpw.git",
    license="MIT",
    packages=find_packages(),
    package_dir={'cloud': 'Cloud', 'yazaki': 'Yazaki'},
    install_requires=[
        'requests'
    ],
    tests_require=[
        'coverage', 'wheel', 'pytest', 'requests_mock'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha"
    ]
)
