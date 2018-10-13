from datetime import datetime
import time
import re
from collections import defaultdict
from datadog import statsd


# mapping between datadog and supervisord log levels
METRIC_TYPES = {
    'AVERAGE_RESPONSE': 'nginx.net.avg_response',
    'FIVE_HUNDRED_STATUS': 'nginx.net.5xx_status',
    'FOUR_HUNDRED_STATUS': 'nginx.net.4xx_status',
    'THREE_HUNDRED_STATUS': 'nginx.net.3xx_status',
    'TWO_HUNDRED_STATUS': 'nginx.net.2xx_status',
}


LOG_FORMAT_PATTERN = [
    ('', ''),
    ('remote_addr', ' - '),
    ('remote_user',' ['),
    ('time_local', '] "'),
    ('request', '" '),
    ('status', ' '),
    ('body_bytes_sent', ' "'),
    ('http_referer', '" "'),
    ('http_user_agent', '" '),
    ('request_time', ' "'),
    ('host', '" "'),
    ('proxy_host', '"'),
]


def parse(line):
    if len(line) == 0:
        return None
    _resp = {}
    for key, stop in LOG_FORMAT_PATTERN:
        index = line.find(stop)
        if key == '':
            continue
        _resp[key] = line[0:index]
        line = line[index + len(stop):]
    return _resp


def normalize(obj):
    obj.update({
        'timestamp': parse_access_log_date_to_timestamp(obj),
        'method': extract_method(obj),
        'request_time': float(obj['request_time']),
        'status': int(obj['status']),
    })
    return obj


def parse_access_log_date_to_timestamp(obj):
    date = datetime.strptime(obj['time_local'][:-6], "%d/%b/%Y:%H:%M:%S")
    date = time.mktime(date.timetuple())
    return date


def extract_method(obj):
    match = re.match('^(GET|POST|HEAD|PATCH|OPTIONS|PUT)', obj['request'])
    if not match:
        return ''
    return match.group(0)


def extract_tags(obj):
    _tags = []
    if not obj['proxy_host'] == '-':
       _tags.append("upstream:{}".format(obj['proxy_host']))
    return _tags


def is_http_status_loggable(obj):
    return 'status' in obj and (599 <= obj['status'] and 200 >= obj['status'])

def get_http_status_metric_name(obj):
    status = obj['status']
    if status >= 200 and status <= 299:
        metric_selector = 'TWO'
    elif status >= 300 and status <= 399:
        metric_selector = 'THREE'
    elif status >= 400 and status <= 499:
        metric_selector = 'FOUR'
    elif status >= 500 and status <= 599:
        metric_selector = 'FIVE'
    else:
        return None
    return '{}_HUNDRED_STATUS'.format(metric_selector)


def should_flush_buffer(buffer):
    if isinstance(buffer, int):
        return buffer >= SAMPLE_RATE
    else:
        return len(buffer) >= SAMPLE_RATE


def generate_http_status_metric(obj):
    metric_name = get_http_status_metric_name(obj)
    if not metric_name:
        return

    statsd.increment(
        METRIC_TYPES[metric_name],
        value=1,
        sample_rate=0.1,
        tags=extract_tags(obj)
    )


def generate_average_response_time_metric(obj):
    statsd.gauge(
        METRIC_TYPES['AVERAGE_RESPONSE'],
        value=obj['request_time'],
        sample_rate=0.1,
        tags=extract_tags(obj)
    )


metrics_to_report = [
    generate_http_status_metric,
    generate_average_response_time_metric,
]


def generate_nginx_metrics(line):
    normalized_log = normalize(parse(line))
    for method in metrics_to_report:
        method(normalized_log)


if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        generate_nginx_metrics(line)
