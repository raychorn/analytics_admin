#language:en
Feature: Perform Job

  So that we can perform long running jobs in the background
  As a Job Performer
  I want use resque to perform and manage jobs

  A HiveJob inherits from Resque::JobWithStatus, when a new HiveJob is created, the job
  is queued by resque to be proccessed once resources are available. Once a resource/worker
  is freedup it executes the code in perform.

  Scenario: