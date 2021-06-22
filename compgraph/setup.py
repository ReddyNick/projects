from setuptools import setup


packages = {
    'compgraph': 'compgraph/'
}

setup(
    name="compgraph",
    version='1',
    author="Nikita Chuzhakin (mango.syrup@yandex.ru)",
    license="GNU General Public License",
    description="MapReduce computational graph library",
    packages=packages,
    package_dir=packages,
    include_package_data=False,
    install_requires=['psutil']
)
