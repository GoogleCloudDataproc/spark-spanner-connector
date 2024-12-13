#!/bin/sh

pycodestyle spannergraph --exclude=tests_gold.py && python3 -m spannergraph.tests
