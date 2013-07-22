#language:en
#TODO: Re-write
Feature:  Create Connection

  In order to communicate with Hive Server
  As an Admin
  I want to create an active connection to Hive Server

  For each connection that you create, you get a unique connection manager for
  that data access object. Invalid connections should return an error and advice the user.

  Background:
    Given I am not yet connected to Hive Server


  Scenario: initialize connection prividing all valid parameters
    When I connect to Hive Server with parameters "hivedev1","10_000","default"
    Then I should recieve "true" on connection manager online

  Scenario: initialize connection providing valid server when database does not exist
    When I connect to Hive Server with parameters "hivedev1","10_000","doesnotexist"
    Then I should recieve "false" on connection manager online 
    And I should recieve a connection error

  Scenario: attempt connection using default values and valid server
    When I connect to Hive Server with parameters "hivedev1"
    Then I should recieve "true" on connection manager online

  Scenario: attempt connection using default values and invalid server
    When I connect to Hive Server with parameters "dummy"
    Then I should recieve "false" on connection manager online
    And I should recieve a connection error