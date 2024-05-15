from setuptools import setup

setup(
    name = 'invest_dash', 
    version = '0.1', 
    author = "Jameson Marshall",
    description = "A web scraper, API, and dashboard for Dual Momentum investing strategy",
    packages=["backend","schemas","jobs"]
)
