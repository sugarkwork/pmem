import setuptools

setuptools.setup(
    name="skpmem",
    version="0.1.4",
    install_requires=["aiosqlite", "setuptools"],
    packages=setuptools.find_packages(),
    description="Persistent Memory",
    author="sugarkwork",
    python_requires='>=3.10',
)
