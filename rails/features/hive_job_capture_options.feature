#language:en
Feature: Capture Options

  In order to process each option and interprete keywords
  As a Job Performer
  I want to capture and parse options in an helper class

  Each HiveJob created to perform a background task/job is followed by options. Each option
  can be formatted differently. The database_name and query_results options are required.
  A query_results options is formatted as a JSON array that follows it's model.

  Scenario: Receive empty options
    Given I am not yet initialize
    When I initialize a new hive job options
    And the options hash include "0" queries
    Then I should recieve a failed error
    And error message should include "options error: options with 0 queries"

  @wip
  Scenario: Receive options with one query
    Given I am not yet initialize
    When I initialize a new hive job options
    And the options hash include "1" queries
    Then I should generate options
    And "QueryOption" should be added to the list
    And The query value should be "select key,value from testHiveDriverTable"

  Scenario: Receive options with multiple queries

  Scenario: Receive options with no database name