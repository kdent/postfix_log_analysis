#!/usr/bin/python
import csv
import sys

#rdr = csv.reader(sys.stdin.read().split('\r'))
rdr = csv.reader(sys.stdin)
out = csv.writer(sys.stdout)
for row in rdr:
    if row[6] == 'Accepted':
        if row[3].endswith(".top") or row[2].endswith(".gdn"):
            row[5] = 'spam'
        elif row[3].endswith('.qq.com'):
            row[5] = 'spam'
        elif row[3].endswith('.us'):
            row[5] = 'spam'
        elif row[3].endswith('.rocks'):
            row[5] = 'spam'
        elif row[3].endswith('.amazonses'):
            row[5] = 'spam'
        elif row[3].endswith('.xyz'):
            row[5] = 'spam'
        elif row[3].endswith('.pro'):
            row[5] = 'spam'
        elif row[3].endswith('.download'):
            row[5] = 'spam'
        elif row[3].endswith('.website'):
            row[5] = 'spam'
        elif row[3].endswith('groups.yahoo.com'):
            row[5] = 'good'
        elif row[3].endswith('bounces.amazon.com'):
            row[5] = 'good'
        elif row[3].endswith('bounce.google.com'):
            row[5] = 'good'
        elif row[3].endswith('nextdoor.com'):
            row[5] = 'good'
        elif row[3].endswith('nytimes.com'):
            row[5] = 'good'
        elif row[3].endswith('theskimm.com'):
            row[5] = 'good'
        elif row[3].endswith('anthropologie.com'):
            row[5] = 'good'
        elif row[3].endswith('operadeparis.fr'):
            row[5] = 'good'
        elif row[3].endswith('firsttechfed.com'):
            row[5] = 'good'
        elif row[3].endswith('ieee.org'):
            row[5] = 'good'
        elif row[3].endswith('linkedin.com'):
            row[5] = 'good'
        elif row[3].endswith('floridapanthers.com'):
            row[5] = 'good'
        elif row[3].endswith('bounces.google.com'):
            row[5] = 'good'
        elif row[3].endswith('coursera.org'):
            row[5] = 'good'
        elif row[3].endswith('tiaa.org'):
            row[5] = 'good'
        elif row[3].endswith('kl-wines.com'):
            row[5] = 'good'
        elif row[3].endswith('quora.com'):
            row[5] = 'good'
        elif row[3].endswith('evgonetwork.com'):
            row[5] = 'good'
        elif row[3].endswith('pinterest.com'):
            row[5] = 'good'
        elif row[3].endswith('columbia.edu'):
            row[5] = 'good'
        elif row[3].endswith('ibmsecu.org'):
            row[5] = 'good'
        elif row[3].endswith('everbridge.net'):
            row[5] = 'good'
    out.writerow(row)
