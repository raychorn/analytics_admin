module JobRunnerStepsHelpers
end

Given /^a JobRunner with "([^"]*)" ResultQuery$/ do |count|
  @job_runner = JobRunner.new
  @job_runner.database_name = "default"
  @job_runner.queries = [QueryResult.new]
end

Given /^ResultQuery with query string "([^"]*)"$/ do |query|
  @job_runner.queries.first.query_string = query
  @job_runner.queries.first.store_in_db = 0
end

When /^I start the job runner$/ do
  @job_runner.save!
  @job_runner.start
end

Then /^I should recieve a job_uuid to later poll DataResult$/ do
  @job_runner.job_uuid.should_not be_nil
end