#!/usr/bin/python
import csv
import sys

rdr = csv.reader(sys.stdin)
out = csv.writer(sys.stdout)
for row in rdr:
    if row[5] == 'Accepted':
        if row[2].endswith(".top") or row[2].endswith(".gdn"):
            row[4] = 'spam'
        elif row[2].endswith('.qq.com'):
            row[4] = 'spam'
        elif row[2].endswith('.xyz'):
            row[4] = 'spam'
        elif row[2].endswith('.pro'):
            row[4] = 'spam'
        elif row[2].endswith('.download'):
            row[4] = 'spam'
        elif row[2].endswith('.website'):
            row[4] = 'spam'
        elif row[2].endswith('groups.yahoo.com'):
            row[4] = 'good'
    out.writerow(row)
        
