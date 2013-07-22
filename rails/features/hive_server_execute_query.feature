#language:en
#TODO: Queries once JSON format is fixed
Feature:  Execute Query

  In order to get results from Hive
  As an Admin Manager
  I want to execute queries to Hive Server

  The hive connection manager should be able to execute queries to the server
  and return either a result or status. When executing to storeQueryOutputToHiveTable
  the return result is a status. When executing to executeQuery the result is JSON.

  Scenario: execute query to get all data from a table as JSON
    Given an active "true" connection manager
    When I execute the query "select key,value from testHiveDriverTable"
    And expect the result in "JSON"
    Then I should recieve the result as "JSON"

  Scenario: execute query to get all data and verify result
    Given an active "true" connection manager
    When I execute the query "select key,value from testHiveDriverTable"
    And expect the result in "JSON"
    Then I should recieve a json result with the following json members:
      | members  |
      | columns  |