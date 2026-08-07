#!/usr/bin/env python3

## @file test_converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 22 Nov 2024
#
##########################################################################
import glob
import importlib.resources
import random
import os
import yaml

from dask.distributed import Client
from dask.distributed import performance_report
from dask.distributed.diagnostics import MemorySampler

import argparse
import matplotlib.pyplot as plt
import numpy as np
import pytest

from crocolaketools.converter.converterArgoQC import ConverterArgoQC
##########################################################################

class TestConverterPerformance:

#------------------------------------------------------------------------------#
## Set of tests for the Converter class constructor, recording performances too
## Goals:
## * it contains performance tests for the whole BGC and PHY dbs: they are
##   demanding and might raise enough warnings to make the browser crash, so the
##   idea is to have them here so that they can easily be run on a cluster
## * it contains a few examples to show how to set up tests for individual or
##   few files at a time

    def test_argo_bgc_qc_single_file(self):
        """
        Test performance to read one file from the Argo BGC parquet
        database, filter by QC, and convert
        """

        nw = 1
        tpw = 1
        client = Client(
            threads_per_worker=tpw,
            n_workers=nw,
            memory_limit='8GB',
            dashboard_address='localhost:35784',
            processes=True
        )

        ConverterBGC = ConverterArgoQC(db_type='bgc')

        ms = MemorySampler()
        basename = "test_convert_performance_argoqc_bgc_01_"+str(nw)+"_"+str(tpw)
        performance_filename = basename + ".html"
        fig_filename = basename + ".png"
        with performance_report(filename=performance_filename):
            ms_name = "argoqc_test_bgc_single_file"
            pq_files = [
                f for f in os.listdir(ConverterBGC.input_path)
                if os.path.isfile(os.path.join(ConverterBGC.input_path, f))
                and f.endswith(".parquet")
            ]
            pq_file = os.path.abspath(
                os.path.join(
                    ConverterBGC.input_path,
                    random.choice(pq_files)
                )
            )
            with ms.sample(ms_name):
                ConverterBGC.convert(pq_file)

        ax = ms.plot(align=True)
        plt.savefig(fig_filename)

        client.shutdown()


    def test_argo_bgc_qc_n_files(self):
        """
        Test performance to read one file from the Argo BGC parquet
        database, filter by QC, and convert
        """

        n = 100

        nw = 1
        tpw = 1
        client = Client(
            threads_per_worker=tpw,
            n_workers=nw,
            memory_limit='8GB',
            dashboard_address='localhost:35784',
            processes=True
        )

        ConverterBGC = ConverterArgoQC(db_type='bgc')

        ms = MemorySampler()
        basename = "test_convert_performance_argoqc_bgc_10_"+str(nw)+"_"+str(tpw)
        performance_filename = basename + ".html"
        fig_filename = basename + ".png"
        with performance_report(filename=performance_filename):
            ms_name = "argoqc_test_bgc_single_file"
            pq_files = [
                f for f in os.listdir(ConverterBGC.input_path)
                if os.path.isfile(os.path.join(ConverterBGC.input_path, f))
                and f.endswith(".parquet")
            ]
            n = np.max([len(pq_files),n])
            pq_files = [
                os.path.abspath(
                    os.path.join(
                        ConverterBGC.input_path, f
                    )
                ) for f in random.sample(pq_files, n)
            ]
            with ms.sample(ms_name):
                ConverterBGC.convert(pq_files)

        ax = ms.plot(align=True)
        plt.savefig(fig_filename)

        client.shutdown()

if __name__ == '__main__':
    main()
