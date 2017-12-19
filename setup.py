from setuptools import setup

setup(
    name = "twitterScrape",
    packages = ["twitterScrape"],
    install_requires = ["argparse", "python-twitter", "unicodecsv", "wordcloud", "requests_oauthlib", "pyquery", "lxml", "pymp-pypi", "gooey==0.9.2.5+js", "pytimeparse", "csvProcess==0.1"],
    dependency_links=["git+https://github.com/jschultz/Gooey.git#egg=gooey-0.9.2.5+js",
                      "git+https://github.com/BarraQDA/csvProcess.git#egg=csvProcess-0.1"],
    python_requires = "<3",
    entry_points = {
        "gui_scripts": ['twitterGui    = twitterScrape.twitterGui:twitterGui',
                        'twitterScrape = twitterScrape.twitterScrape:main',
                        'twitterSearch = twitterScrape.twitterSearch:main']
        },
    version = "0.1",
    description = "Python scripts to scrape and manipulate Twitter data",
    author = "Jonathan Schultz",
    author_email = "jonathan@schultz.la",
    license = "GPL3",
    classifiers = [
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GPL3 License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 2",
        "Intended Audience :: End Users/Desktop",
        ],
    )
