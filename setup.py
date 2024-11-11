import codecs
import setuptools

with codecs.open('requirements.txt', 'r', encoding='utf-16') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="PersistentMemory",
    version="0.1.2",
    install_requires=requirements,
    packages=setuptools.find_packages(),
    description="Persisten Memory",
    author="sugarkwork",
    python_requires='>=3.10',
)
