#!/usr/bin/python
import csv
import sys

rdr = csv.reader(sys.stdin)
out = csv.writer(sys.stdout)
for row in rdr:
    if row[2].endswith(".top") or row[2].endswith(".gdn"):
        row[4] = 'spam'
    elif row[2].endswith('groups.yahoo.com'):
        row[4] = 'good'
    out.writerow(row)
        
