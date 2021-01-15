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
    
    def test_auth(self):
        test_auth = pap.auth('./test_auth.txt')
        self.assertEqual(test_auth.client_id, '555ABCD111')
        self.assertEqual(test_auth.client_secret, '1j5tr4katesttest')
        self.assertEqual(test_auth.user_agent, 'test_agent_please')



if __name__ == '__main__':
    unittest.main()