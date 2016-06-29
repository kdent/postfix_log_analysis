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
    out.writerow(row)
        
