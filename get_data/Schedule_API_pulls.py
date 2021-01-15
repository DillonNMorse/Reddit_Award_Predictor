# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 14:51:49 2020

@author: Dillo
"""

from API_pull_and_process import get_reddit_submissions
from apscheduler.schedulers.blocking import BlockingScheduler



# Run once at the beginning
#get_reddit_submissions(sortedby = ['new', 'hot', 'rising'], num_posts = 500)
  

# Run every 2.75 hours
scheduler = BlockingScheduler()
@scheduler.scheduled_job('interval', hours = 5)
def timed_job():
    get_reddit_submissions(sortedby = ['new', 'hot', 'rising'], num_posts = 700)
scheduler.start()

