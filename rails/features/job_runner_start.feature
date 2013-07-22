#language:en
Feature: Start Job

  So that a job is successfully processed through analytics
  As a Job with Query Results
  I want to start a job and send it to perform

  The portal and admin both share an ActiveResource Job_Runner. The portal populates the
  instance with Result Queries and later a start command is sent to Job Runner. The
  Job runner then calls a perform method from a derived Resque class in the background.

  Scenario: Start a job with single hive query
    Given a JobRunner with "1" ResultQuery
    And ResultQuery with query string "select key,value from testHiveDriverTable"
    When I start the job runner
    Then I should recieve a job_uuid to later poll DataResult

  Scenario: Start a job with two hive queries

  Scenario: Start a job with multiple hive queries

  Scenario: Start a job with no query results