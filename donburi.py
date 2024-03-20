#!/usr/bin/env python

import sh
from time import sleep
from contextlib import redirect_stdout
import sys
import argparse
import pyodbc
from tabulate import tabulate
import signal
import pprint
import os
import datetime
import time
import dateutil.parser
from contextlib import redirect_stdout
from io import StringIO

import ramenya2

# READONLY database
statusdbr_ = pyodbc.connect("DSN=ProductionStatus")
statusdbr = statusdbr_.cursor()

from flask import Flask

app = Flask(__name__)

@app.route("/runs")
def jobs_by_run():
    result=StringIO()
    with redirect_stdout(result):
        ramenya2.noodles( ["--html","query","--reports","runs","--timestart","03/10/2024"])
    return result.getvalue()

@app.route("/clusters")
def jobs_by_clusters():
    result=StringIO()
    with redirect_stdout(result):
        ramenya2.noodles( ["--html","query","--reports","clusters","--timestart","03/10/2024"])
    return result.getvalue()

@app.route("/pending")
def pending_jobs():
    result=StringIO()
    with redirect_stdout(result):
        ramenya2.noodles( ["--html","query","--reports","pending","--timestart","03/10/2024"])
    return result.getvalue()

@app.route("/failed")
def failed_jobs():
    result=StringIO()
    with redirect_stdout(result):
        ramenya2.noodles( ["--html","query","--reports","failed","--timestart","03/10/2024"])
    return result.getvalue()


@app.route("/started")
def started_jobs():
    result=StringIO()
    with redirect_stdout(result):
        ramenya2.noodles( ["--html","query","--reports","started","--timestart","03/10/2024"])
    return result.getvalue()


