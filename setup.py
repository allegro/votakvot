import setuptools

setuptools.setup(
    name="votakvot",
    version="0.1.dev0",
    description="Track information during code testing and researching.",
    author="Andrei Zhlobich",
    author_email="andrei.zhlobich@allegro.pl",
    packages=[
        "votakvot",
        "votakvot.extras",
    ],
    python_requires=">=3.7.1,<4.0.0",
    # TODO: Add version ranges.
    install_requires=[
        "pyyaml",
        "fsspec",
        "multiprocess",
        "pandas",
        "wrapt",
    ],
    extras_require={
        "gcm": ["google-cloud-monitoring", "gcsfs"],
        "beam": ["apache-beam", "gcsfs"],
        "prometheus": ["prometheus-client"],
    },
)
