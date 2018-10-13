import unittest
import pprint

from datadog_ngx_reporter import (
    parse,
    normalize,
    generate_http_status_metric,
    generate_average_response_time_metric,

    get_http_status_metric_name,
    generate_nginx_metrics,
    METRIC_TYPES,
    extract_tags,
)


LOG_FORMAT_TIME_COMBINED = '1.1.1.1 - - [01/Oct/2018:06:36:17 +0200] "GET /telephone/4 HTTP/1.1" 200 5 "-" "Outlook-i" 0.018 "domain.com" "upstream-name"'


class TestNGiNXParse(unittest.TestCase):
    def setUp(self):
        self.parsed_log = parse(LOG_FORMAT_TIME_COMBINED)

    def test_should_find_remote_addr(self):
        self.assertEqual(self.parsed_log['remote_addr'], '1.1.1.1')
        self.assertEqual(self.parsed_log['status'], '200')
        self.assertTrue(all(k in self.parsed_log for k in (
            'time_local',
            'remote_addr',
            'proxy_host',
            'host',
        )))

    def test_normalize(self):
        normalized = normalize(self.parsed_log)

        self.assertTrue(all(k in normalized for k in (
            'time_local',
            'remote_addr',
            'method',
            'timestamp',
            'request_time',
        )))

        self.assertTrue(normalized['method'] == 'GET')
        self.assertTrue(normalized['status'] == 200)
        self.assertTrue(normalized['timestamp'] == 1538368577.0)
        self.assertTrue(normalized['request_time'] == 0.018)
        self.assertTrue(self.parsed_log['proxy_host'] == 'upstream-name')

    def test_aggregation(self):
        normalized = normalize(self.parsed_log)

        self.assertEqual('TWO_HUNDRED_STATUS', get_http_status_metric_name(normalized))
        self.assertEqual('THREE_HUNDRED_STATUS', get_http_status_metric_name({'status': 300}))
        self.assertEqual('FOUR_HUNDRED_STATUS', get_http_status_metric_name({'status': 400}))
        self.assertEqual('FIVE_HUNDRED_STATUS', get_http_status_metric_name({'status': 500}))
