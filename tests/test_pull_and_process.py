# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 13:12:37 2021

@author: Dillo
"""

import sys
sys.path.append('../get_data')

import unittest
import pull_and_process as pap


class TestPap(unittest.TestCase):
    
    def test_get_auth(self):
        test_auth = pap.get_auth('./test_auth.txt')
        self.assertEqual(test_auth.client_id, '555ABCD111')
        self.assertEqual(test_auth.client_secret, '1j5tr4katesttest')
        self.assertEqual(test_auth.user_agent, 'test_agent_please')

    def test_subreddit_list(self):
        test_subreddit_list = (pap
                               .get_subreddit_list('./test_subreddit_list.txt')
                               )
        validation_list = ['subreddit1', 'subreddit2', 'subreddit3']
        self.assertEqual(test_subreddit_list, validation_list)


if __name__ == '__main__':
    unittest.main()