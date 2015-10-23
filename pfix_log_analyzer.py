#!/usr/bin/python

from datetime import datetime, date, time
import re
import sys

line_count = 0
cur_year = datetime.now().year
month_to_int = {}
month_to_int['Jan'] = 1
month_to_int['Feb'] = 2
month_to_int['Mar'] = 3
month_to_int['Apr'] = 4
month_to_int['May'] = 5
month_to_int['Jun'] = 6
month_to_int['Jul'] = 7
month_to_int['Aug'] = 8
month_to_int['Sep'] = 9
month_to_int['Oct'] = 10
month_to_int['Nov'] = 11
month_to_int['Dec'] = 12

log_line_pattern = re.compile(".*postfix/(anvil|cleanup|master|postfix-script|qmgr|smtp|smtpd)\[(\d+)\]: (.*)")
host_and_ip_pattern = re.compile('(\S+)\[([\d\.]+)\]')
ip_pattern = re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:{0,1}')
qid_pattern = re.compile('[0-9A-Z]{10}:{0,1}')
msg_id_pattern = re.compile('message-id=<(.*)>')

msg_data = {}
queue_id_index = {}
# connection_timestamp
# connection_host
# connection_ip
# disconnect_timestamp
# queue_id
# from_address
# to_address
# helo_host
# message_id (ID|NOQUEUE)
# delivery_status (reject)
# delivery_status_msg

def print_record(msg):
    print "**********************************"
    print "connect_time:", msg.get('connect_time', '')
    print "connection_host:", msg.get('connection_host', '')
    print "connection_ip:", msg.get('connection_ip', '')
    print "disconnect_time:", msg.get('disconnect_time', '')
    print "queue_id:", msg.get('queue_id', '')
    print "from_address:", msg.get('from_address', '')
    print "to_address:", msg.get('to_address', '')
    print "helo_host:", msg.get('helo_host', '')
    print "message_id:", msg.get('message_id', '')
    print "delivery_status:", msg.get('delivery_status', '')
    print "delivery_status_msg:", msg.get('delivery_status_msg', '')

def match_token(tgt, token_list):
    tok = token_list.pop(0)
    if tok == tgt:
        return tok
    elif tgt == "ANY":   # wildcard matching
        return tok
    elif tgt == "IPADDR" and ip_pattern.match(tok):
        return tok
    elif tgt == "QID" and qid_pattern.match(tok):
        return tok
    else:
        raise ValueError("parsing error at line", line_count, ": expecting", tgt, "got", tok)

def lookahead(token_list):
    return token_list[0]

def get_unique_id(queue_id):
    unique_id = queue_id_index.get(queue_id, None)
#    unique_id = None
#    try:
#        unique_id = queue_id_index[queue_id]
#    except KeyError as e:
#        raise ValueError("cannot find unique ID value for queue ID", queue_id, "at line", line_count)
    return unique_id

# Extract host and IP from patterns like fg5xc.lsbg.download[94.156.37.9]
def match_host_ip(str):

    if str == 'unknown[unknown]':    # check the special case
        return ('unknown', 'unknown')

    m = host_and_ip_pattern.match(str)
    if not m:
        raise ValueError("parse error: couldn't match a host and IP address in", str, "at line", line_count)
    host_name = m.group(1)
    ip_addr = m.group(2)
    return (host_name, ip_addr)

def match_message_id(str):
    m = msg_id_pattern.match(str)
    if m:
        return(m.group(1))
    else:
        raise ValueError("parse error: expecting message ID, line", line_count, "with input:", str)

def match_reject_msg(pid, token_list):
    # Make the sequence of tokens a string to do regex matching.
    line = " ".join(token_list)
    m = re.match('(.*);(.*)', line)
    reject_msg = m.group(1)
    nv_pairs_str = m.group(2)
    ip_addr = None
    msg_token_list = reject_msg.split()
    if match_token('RCPT', token_list):
        match_token('from', token_list)
        tok = match_token('ANY', token_list)
        if tok:
            (host, ip_addr) = match_host_ip(tok)
    if ip_addr:
        unique_id = pid + ip_addr
        msg_data[unique_id]['queue_id'] = "NOQUEUE"
        msg_data[unique_id]['delivery_status'] = "reject"
        msg_data[unique_id]['delivery_status_msg'] = reject_msg[reject_msg.index(':')+2:]
        for nv_pair in nv_pairs_str.split():
            try:
                (name, value) = nv_pair.split("=", 1)
            except ValueError as e:
                print "parsing error at line", line_count, "with input:", nv_pairs_str
                raise
            # Assign attributes eliminating angle brackets.
            if name == "from":
                msg_data[unique_id]["from_address"] = value[1:-1]
            if name == "to":
                msg_data[unique_id]["to_address"] = value[1:-1]
            if name == "helo":
                msg_data[unique_id]["helo_host"] = value[1:-1]

def match_smtpd(timestamp, pid, token_list):

    next_token = match_token('ANY', token_list)
    if next_token == 'connect':
        # Initial connection, start a new record. The connection host
        # IP address plus the daemon's process ID uniquely identify
        # this connection.
        if match_token('from', token_list):
            tok = match_token('ANY', token_list)
            (host_name, ip_addr) = match_host_ip(tok)
            unique_id = pid + ip_addr
            msg_data[unique_id] = {}
            msg_data[unique_id]["connection_host"] = host_name
            msg_data[unique_id]["connect_time"] = timestamp
            msg_data[unique_id]["connection_ip"] = ip_addr
    elif next_token == 'disconnect':
        # Disconnect, print the record and clear it.
        if match_token('from', token_list):
            tok = match_token('ANY', token_list)
            (host_name, ip_addr) = match_host_ip(tok)
            unique_id = pid + ip_addr
            msg_data[unique_id]["disconnect_time"] = timestamp
            qid = msg_data.get('queue_id')
            if qid == 'NOQUEUE' and msg_data[unique_id].get("delivery_status") != "lost":
                print_record(msg_data[unique_id])

    elif next_token == 'NOQUEUE:':
        match_token('reject:', token_list)
        match_reject_msg(pid, token_list)
    elif next_token == 'warning:':
        return        # ignore warning message
    elif next_token == 'lost':   # lost connection
        match_token('connection', token_list)
        match_token('after', token_list)
        match_token('ANY', token_list)
        tok = lookahead(token_list)
        if tok == '(approximately':
            match_token('(approximately', token_list)
            match_token('ANY', token_list)
            match_token('bytes)', token_list)
        match_token('from', token_list)
        next_token = match_token('ANY', token_list)
        (host, ip_addr) = match_host_ip(next_token)
        unique_id = pid + ip_addr
        msg_data[unique_id]['delivery_status'] = "lost"
    elif next_token == 'timeout':
        match_token('after', token_list)
        return # ignore timeout warning messages
    elif qid_pattern.match(next_token):
        queue_id = next_token[0:-1]
        next_token = match_token('ANY', token_list)
        (client_label, host_ip_str) = next_token.split('=')
        (host_name, ip_addr) = match_host_ip(host_ip_str)
        unique_id = pid + ip_addr
        msg_data[unique_id]['queue_id'] = queue_id
        queue_id_index[queue_id] = unique_id
    else:
        token_list.insert(0, next_token)
        print "SMTPD", token_list

def match_smtp(token_list):
    print "SMTP:", token_list
    next_token = match_token('ANY', token_list)
    if next_token == 'connect':
        match_token('to', token_list)
    elif qid_pattern.match(next_token):
        queue_id = next_token[0:-1]
        unique_id = get_unique_id(queue_id)

        next_token = match_token('ANY', token_list)
        # TODO: do some checking here to make sure it's correct.
        (to_label, to_addr) = next_token.split("=", 1)
        if unique_id:     # TODO: temporary
            msg_data[unique_id]['to_address'] = to_addr[1:-2]
            print_record(msg_data[unique_id])
        

def match_qmgr(token_list):
    queue_id = match_token('QID', token_list)[:-1]
    unique_id = queue_id_index.get(queue_id)
    if not unique_id:
#            raise ValueError("parse error: unable to locate message record for queue ID", queue_id, "at line", line_count, "in log")
        # TODO: for messages that are created internally, process smtp lines to create a msg_data record for now, just ignore
        return

    tok = lookahead(token_list)
    if tok == 'removed':
        print_record(msg_data[unique_id])
        # TODO: clear msg_data record
    elif qid_pattern.match(tok):
        (from_label, from_addr) = tok.split('=', 1)
        msg_data[unique_id]['from_address'] = from_addr[1:-1]
        msg_data[unique_id]['delivery_status'] = "queued for delivery"
        # TODO: add code to capture message size and number of recipients

def match_cleanup(token_list):
    queue_id = match_token('QID', token_list)[:-1]
    unique_id = queue_id_index.get(queue_id)
    if not unique_id:
#            raise ValueError("parse error: unable to locate message record for queue ID", queue_id, "at line", line_count, "in log")
            # TODO: smtp record required here. Put this back when that's done. For now, ignore.
        return

    next_token = match_token('ANY', token_list)
    msg_id = match_message_id(next_token)
    msg_data[unique_id]['message_id'] = msg_id

def match_timestamp(line):
    (month, day, timestr, host, remainder) = line.split(None, 4)
    (hour, minute, seconds) = [int(i) for i in timestr.split(':')]
    dt = date(int(cur_year), month_to_int[month], int(day))
    tm = time(hour, minute, seconds)
    timestamp = datetime.combine(dt, tm)
    return timestamp

def parse(line):
    timestamp = match_timestamp(line)
    # Determine which Postfix program generated the log entry.
    m = log_line_pattern.match(line)
    if not m:
        print "ERROR: can't find postfix program in", line
        return
    program = m.group(1)
    pid = m.group(2)
    remainder = m.group(3)
    if program == "anvil" or program == "master" or program == "postfix-script":
        return    # ignore non-message handling programs

    token_list = remainder.split()
    if program == "smtpd":
        match_smtpd(timestamp, pid, token_list)
    elif program == "smtp":
        match_smtp(token_list)
    elif program == "qmgr":
        match_qmgr(token_list)
    elif program == "cleanup":
        match_cleanup(token_list)
    else:
        print "**UNKNOWN postfix command", program

#
# Start program execution
#
if len(sys.argv) != 2:
    print "usage: pfix_log_analyzer.py <log file>"
    sys.exit(1)

logfile = sys.argv[1]
line_count = 0
with open(logfile, 'r') as f:
    for line in f:
        line_count += 1
        parse(line)


