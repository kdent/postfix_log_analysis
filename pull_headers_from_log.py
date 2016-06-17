#!/usr/bin/python

import csv
import re
import sys

pattern = re.compile(".*cleanup\[\d+\]: ([0-9A-Z]{10}): warning: header (From:|Subject:) (.*) from (\S+)\[([\d\.]+)\]; .*")
record = {}
out = csv.writer(sys.stdout)

for line in sys.stdin:
    m = pattern.match(line)
    if m:
        if m.group(1) not in record:
            record[m.group(1)] = {}
        record[m.group(1)][m.group(2)] = m.group(3)

out.writerow(['Queue ID', 'From Header', 'Subject'])
for qid in record.keys():
    out.writerow([qid, record[qid]['From:'], record[qid]['Subject:']])

