# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 17:54:27 2021

@author: Dillo
"""

class ApiRetryAttemptsExceeded(Exception):
    """Raise when the API continues to time out, data not gathered"""
    pass

class InsufficientDataPassed(Exception):
    """Raise when the combination of function arguments passed are insuffucient
    to complete the function task."""
    pass