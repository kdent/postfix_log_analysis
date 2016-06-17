#!/usr/bin/python

from datetime import datetime, date, time
import csv
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

log_line_pattern = re.compile(".*postfix/(anvil|cleanup|master|postfix-script|qmgr|smtp|smtpd|scache|pickup|local|bounce|postsuper)\[(\d+)\]: (.*)")
host_and_ip_pattern = re.compile('(\S+)\[([\d\.]+)\]')
ip_pattern = re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:{0,1}')
qid_pattern = re.compile('[0-9A-Z]{10}:{0,1}')
msg_id_pattern = re.compile('(resent-)?message-id=<(.*)>')

msg_data = {}
queue_id_index = {}
# connection_date
# connection_time
# connecting_host
# connecting_ip
# disconnect_timestamp
# queue_id
# from_address
# to_address
# helo_host
# message_id (ID|NOQUEUE)
# delivery_status (reject)
# delivery_status_msg

def print_record(msg):
#    print "**********************************"
#    print "connect_date:", msg.get('connect_date', '')
#    print "connect_time:", msg.get('connect_time', '')
#    print "connecting_host:", msg.get('connecting_host', '')
#    print "connecting_ip:", msg.get('connecting_ip', '')
#    print "disconnect_time:", msg.get('disconnect_time', '')
#    print "queue_id:", msg.get('queue_id', '')
#    print "from_address:", msg.get('from_address', '')
#    print "to_address:", msg.get('to_address', '')
#    print "helo_host:", msg.get('helo_host', '')
#    print "message_id:", msg.get('message_id', '')
#    print "relay_host:", msg.get('relay_host', '')
#    print "relay_ip:", msg.get('relay_ip', '')
#    print "delivery_status:", msg.get('delivery_status', '')
#    print "delivery_status_msg:", msg.get('delivery_status_msg', '')
    csvout.writerow([msg.get('connect_date', ''),
        msg.get('connect_time', ''),
        msg.get('connecting_host', ''),
        msg.get('connecting_ip', ''),
        msg.get('disconnect_time', ''),
        msg.get('queue_id', ''),
        msg.get('from_address', ''),
        msg.get('to_address', ''),
        msg.get('helo_host', ''),
        msg.get('message_id', ''),
        msg.get('relay_host', ''),
        msg.get('relay_ip', ''),
        msg.get('delivery_status', ''),
        msg.get('delivery_status_msg', '')
    ])

class ParseError(Exception):
    def __init__(self, line_count, value):
        self.value = value + " at line: %d" % line_count
    def __str__(self):
        return repr(self.value)

def match_token(tgt, token_list):
    tok = token_list.pop(0)
    if tok == tgt:
        return tok
    elif tgt == "ANY":   # wildcard matching
        return tok
    elif tgt == "IPADDR" and ip_pattern.match(tok):
        return tok
    elif tgt == "QID" and qid_pattern.match(tok):
        return tok[:-1]
    elif tgt == "MSGID":
        m = msg_id_pattern.match(tok)
        return(m.group(2))
    else:
        raise ParseError(line_count, "expecting '%s' got '%s'" % (tgt, tok))

def push_token(tok, token_list):
    token_list.insert(0, tok)

def lookahead(token_list):
    return token_list[0]

def get_unique_id(queue_id):
    unique_id = queue_id_index.get(queue_id, None)
    return unique_id

# Build a hash map of name value pairs
def match_nv_pairs(str):

    # Make a simple state transition table
    # States are (0 ON_NAME), (1 ON_VALUE), (2 IN_BRACKET), (3 BETWEEN)
    # Alphabet: { char, open (<), close (>), equal, space, comma }
    state = [
        {'char': 0, 'open': 0, 'close': 0, 'equal': 1, 'space': 0, 'comma': 0},
        {'char': 1, 'open': 2, 'close': 1, 'equal': 1, 'space': 3, 'comma': 3},
        {'char': 2, 'open': 2, 'close': 1, 'equal': 2, 'space': 2, 'comma': 2},
        {'char': 0, 'open': 0, 'close': 0, 'equal': 0, 'space': 3, 'comma': 3}
    ]
    cur_state = 0
    next_state = 0
    alpha = ''

    nv_pairs = {}
    cur_name = ''
    cur_str = ''
    for c in str:
        if c == '<': alpha = 'open'
        elif c == '>': alpha = 'close'
        elif c == '=': alpha = 'equal'
        elif c == ' ': alpha = 'space'
        elif c == ',': alpha = 'comma'
        else: alpha = 'char'

        next_state = state[cur_state][alpha]
        if cur_state != next_state: # Check for a state transition
            if cur_state == 0:      # ON_NAME
                cur_name = cur_str
                cur_str = ''
            elif cur_state == 3:    # BETWEEN
                nv_pairs[cur_name] = cur_str
                cur_name = ''
                cur_str = c
        else:
            # TODO: FIX: spaces are getting dropped in bracketed values.
            if alpha == 'char' or (state == 2 and alpha == 'space'):
                cur_str += c
        cur_state = next_state
    if len(cur_name) > 0 and len(cur_str) > 0:
        nv_pairs[cur_name] = cur_str

    return nv_pairs

# Extract host and IP from patterns like fg5xc.lsbg.download[94.156.37.9]
def match_host_ip(str):

    if str == 'unknown[unknown]':    # check the special cases
        return ('unknown', 'unknown')
    elif str == 'none':
        return ('none', 'none')

    m = host_and_ip_pattern.match(str)
    if not m:
        raise ParseError(line_count, "parse error: couldn't match a host and IP address in [%s]" % str)
    host_name = m.group(1)
    ip_addr = m.group(2)
    return (host_name, ip_addr)

def match_reject_msg(unique_id, token_list):
    # Make the sequence of tokens a string to do regex matching.
    line = " ".join(token_list)
    m = re.match('(.*);(.*)', line)
    reject_msg = m.group(1)
    nv_pairs_str = m.group(2)

    msg_data[unique_id]['queue_id'] = "NOQUEUE"
    msg_data[unique_id]['delivery_status'] = "reject"
#    msg_data[unique_id]['delivery_status_msg'] = reject_msg[reject_msg.index(':')+2:]
    msg_data[unique_id]['delivery_status_msg'] = reject_msg
    nv_pair_map = match_nv_pairs(nv_pairs_str)
    msg_data[unique_id]["from_address"] = nv_pair_map.get('from', '')
    msg_data[unique_id]["to_address"] = nv_pair_map.get('to', '')
    msg_data[unique_id]["helo_host"] = nv_pair_map.get('helo', '')

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
            if unique_id not in msg_data:
                msg_data[unique_id] = {}
            msg_data[unique_id] = {}
            msg_data[unique_id]["connecting_host"] = host_name
            msg_data[unique_id]["connect_date"] = timestamp.date()
            msg_data[unique_id]["connect_time"] = timestamp.time()
            msg_data[unique_id]["connecting_ip"] = ip_addr
    elif next_token == 'disconnect':
        # Disconnect, print the record and clear it.
        if match_token('from', token_list):
            tok = match_token('ANY', token_list)
            (host_name, ip_addr) = match_host_ip(tok)
            unique_id = pid + ip_addr
            if unique_id not in msg_data:
                msg_data[unique_id] = {}
            msg_data[unique_id]["disconnect_time"] = timestamp
            if msg_data[unique_id].get('delivery_status') == 'reject':
                print_record(msg_data[unique_id])
                del msg_data[unique_id]
    elif next_token == 'NOQUEUE:':
        match_token('reject:', token_list)
        match_token('ANY', token_list)
        match_token('from', token_list)
        (host_name, ip_addr) = match_host_ip(match_token('ANY', token_list))
        unique_id = pid + ip_addr
        match_reject_msg(unique_id, token_list)
        msg_data[unique_id]['queue_id'] = 'NOQUEUE'
    elif next_token == 'warning:':
        return        # ignore warning message
    elif next_token == 'table':
        return        # ignore message about tables changing
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
        # TODO: record this in the message record
    elif next_token == 'timeout':
        match_token('after', token_list)
        return # ignore timeout warning messages
    elif next_token == 'too':
        match_token('many', token_list)
        match_token('errors', token_list)
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
        msg_data[unique_id]['delivery_status'] = 'reject'
        msg_data[unique_id]['delivery_status_msg'] = 'too many errors'
    elif qid_pattern.match(next_token):
        queue_id = next_token[0:-1]
        next_token = match_token('ANY', token_list)
        if next_token == 'reject:':
            match_token('ANY', token_list)
            match_token('from', token_list)
            (host_name, ip_addr) = match_host_ip(match_token('ANY', token_list))
            unique_id = pid + ip_addr
            match_reject_msg(unique_id, token_list)
            msg_data[unique_id]['queue_id'] = queue_id
        else:
            (client_label, host_ip_str) = next_token.split('=')
            (host_name, ip_addr) = match_host_ip(host_ip_str)
            unique_id = pid + ip_addr
            msg_data[unique_id]['queue_id'] = queue_id
            queue_id_index[queue_id] = unique_id
    elif next_token == 'fatal:':
        if match_token('open', token_list):
            return      # smtpd daemon has an error opening a db file
    else:
        token_list.insert(0, next_token)
        raise ParseError(line_count, "Unrecognized token '%s' in %s" % (next_token, token_list))

#
# Grammar:
#   connect to HOST_IP
#   QID NVPAIR [host HOST_IP said: TEXT_MSG EOL|(, NVPAIR)*]
#
def match_smtp(token_list):

    next_token = lookahead(token_list)
    if next_token == 'connect':
        match_token('connect', token_list)
        match_token('to', token_list)
    elif next_token == 'warning:':
        return      # ignore warning message
    else:
        queue_id = match_token('QID', token_list)
        unique_id = get_unique_id(queue_id)
        if not unique_id:
            raise ParseError(line_count,
                "no unique_id for queue_id %s" % queue_id)
        next_token = lookahead(token_list)
        if next_token == 'host':
            match_token('host', token_list)
            next_token = match_token('ANY', token_list)
            (host, ip_addr) = match_host_ip(next_token)
            match_token('said:', token_list)
            msg_data[unique_id]['queue_id'] = queue_id
            msg_data[unique_id]['delivery_status'] = 'not sent'
            msg_data[unique_id]['delivery_status_msg'] = ' '.join(token_list)
        else:
            if not unique_id in msg_data:
                msg_data[unique_id] = {}
            msg_data[unique_id]['queue_id'] = queue_id
            log_line = ' '.join(token_list)
            nv_pairs = match_nv_pairs(log_line)
            msg_data[unique_id]['to_address'] = nv_pairs.get('to','')
            (relay_host, relay_ip) = match_host_ip(nv_pairs.get('relay', ''))
            msg_data[unique_id]['relay_host'] = relay_host
            msg_data[unique_id]['relay_ip'] = relay_ip
            msg_data[unique_id]['delivery_status'] = nv_pairs.get('status', '')
            m = re.search("\((.+)\)", log_line)
            if m:
                msg_data[unique_id]['delivery_status_msg'] = m.group(1)
        print_record(msg_data[unique_id])

def match_qmgr(token_list):
    queue_id = match_token('QID', token_list)
    unique_id = get_unique_id(queue_id)
    if not unique_id:
        unique_id = queue_id  # messages that originate in the system
        queue_id_index[queue_id] = unique_id
        msg_data[unique_id] = {}

    tok = lookahead(token_list)
    if tok == 'removed':
#        del msg_data[unique_id]
        # TODO: figure out when it's safe to delete a record.
        0
    else:
        token_list_str = ' '.join(token_list)
        nv_pairs = match_nv_pairs(token_list_str)
        msg_data[unique_id]['queue_id'] = queue_id
        msg_data[unique_id]['from_address'] = nv_pairs.get('from', '')
        # TODO: add code to capture message size and number of recipients

#
# Grammar:
#   QID reject TEXT_MSG EOL
#   QID MSG_ID
#
# pickup and cleanup are allowed to generate unique IDs based on the queue ID
def match_cleanup(token_list):
    next_token = lookahead(token_list)
    if next_token == 'warning:':
        return
    queue_id = match_token('QID', token_list)
    unique_id = get_unique_id(queue_id)
    if not unique_id:
        unique_id = queue_id  # messages that originate in the system
        queue_id_index[queue_id] = unique_id
        msg_data[unique_id] = {}

    next_token = lookahead(token_list)
    if next_token == "warning:":
        return
    msg_data[unique_id]['queue_id'] = queue_id
    if next_token == "reject:":
        match_token('reject:', token_list)
        msg_data[unique_id]['delivery_status'] = "reject"
        msg_data[unique_id]['delivery_status_msg'] = " ".join(token_list)
    else:
        print token_list
        msg_data[unique_id]['message_id'] = match_token('MSGID', token_list)

#
# pickup and cleanup are allowed to generate unique IDs based on the queue ID
#
def match_pickup(pid, token_list):
    queue_id = match_token('QID', token_list)
    unique_id = get_unique_id(queue_id)
    if not unique_id:
        unique_id = queue_id
        queue_id_index[queue_id] = unique_id
        msg_data[unique_id] = {}

    pairs = match_nv_pairs(' '.join(token_list))
    msg_data[unique_id]['queue_id'] = queue_id
    msg_data[unique_id]['from_address'] = pairs['from']

def match_local(pid, token_list):
    queue_id = match_token('QID', token_list)
    unique_id = get_unique_id(queue_id)
    if not unique_id:
        raise ParseError(line_count, "no unique_id for queue_id %s" % queue_id)
    line = ' '.join(token_list)
    pairs = match_nv_pairs(line)
    msg_data[unique_id]['to_address'] = pairs['to']
    msg_data[unique_id]['queue_id'] = queue_id
    msg_data[unique_id]['delivery_status'] = pairs['status']
    m = re.search("\((.+)\)", line)
    if m:
        msg_data[unique_id]['delivery_status_msg'] = m.group(1)
    if msg_data[unique_id]['delivery_status'] == 'sent':
        print_record(msg_data[unique_id])

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
    if not m:       # Not a Postfix line (that we care about)
        return
    program = m.group(1)
    pid = m.group(2)
    remainder = m.group(3)

    token_list = remainder.split()
    if program == "smtpd":
        match_smtpd(timestamp, pid, token_list)
    elif program == "smtp":
        match_smtp(token_list)
    elif program == "qmgr":
        match_qmgr(token_list)
    elif program == "cleanup":
        match_cleanup(token_list)
    elif program == "pickup":
        match_pickup(pid, token_list)
    elif program == "local":
        match_local(pid, token_list)

#
# Start program execution
#
fh = None
logfile = None
if not sys.stdin.isatty():
    fh = sys.stdin
elif len(sys.argv) != 2:
    print "usage: pfix_log_analyzer.py <log file>"
    sys.exit(1)
else: 
    logfile = sys.argv[1]
    fh = open(logfile, 'r')

line_count = 0
csvout = csv.writer(sys.stdout)
csvout.writerow(['Connection Date',
    'Connection Time',
    'Connecting Host',
    'Connecting IP',
    'Disconnect Timestamp',
    'Queue ID',
    'From Address',
    'To Address',
    'Helo Host',
    'Message ID',
    'Relay Host',
    'Relay IP',
    'Delivery Status',
    'Delivery Status Msg'
])

for line in fh:
    line_count += 1
    parse(line)

