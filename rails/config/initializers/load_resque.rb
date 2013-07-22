require 'resque'
require 'resque/job_with_status'
require 'resque_scheduler'

Resque::Status.expire_in = (24 * 60 * 60 * 30) # 24hrs in seconds