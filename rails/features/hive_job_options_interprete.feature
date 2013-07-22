#language:en
Feature: Interprete Option Keywords

  In order to interprete keywords
  As a Job Performer
  I parse options entries and replace keywords with variables

  Certain values passed into options can contain "keywords" used by the admin to determine
  the value of tables, or dates. For example one of the keywords is #PARENT, this value
  would get replace by the parent query/table.

  Scenario: Query with no keywords
