#!/usr/bin/python

import csv
import sys

if len(sys.argv) < 2:
    print("usage: %s <filename>" % sys.argv[0])
    sys.exit(1)


csvout = csv.writer(sys.stdout)
csvout.writerow(['Connection Date','Connection Time', 'From Address', 'To Address', 'Classification', 'Status'])

with open(sys.argv[1], 'rU') as fh:
    csvrdr = csv.reader(fh)
    for row in csvrdr:
        if row[7] not in ['jdent@seaglass.com', 'kdent@seaglass.com', 'becky@seaglass.com']:
            continue;
        if row[12] == 'reject':
            if 'blocked using zen.spamhaus.org' in row[13]:
                status = 'Spamhaus Reject'
            elif 'Helo command rejected' in row[13]:
                status = 'Helo Reject'
            elif '550-Verification failed for' in row[13]:
                status = 'Bluehost Reject'
            elif 'Sender address rejected' in row[13]:
                status = 'Rule Reject'
            else:
                status = row[13]
        elif row[12] == 'sent':
            status = 'Accepted'
        else:
            status = row[13]
            
        newrow = [row[0], row[1], row[6], row[7], '', status]
        csvout.writerow(newrow)
