
# -- hive_job_capture_options
Given /^I am not yet initialize$/ do
  @hive_job_options = nil
end

When /^I initialize a new hive job options$/ do
  @hive_job_options = Jobs::HiveJobOptions.new
end

When /^the options hash include "([^"]*)" queries$/ do |num|
  begin
    @options = HiveJobSpecHelpers::create_run_options(num)
    @hive_job_options.options = @options
  rescue => e
    @err = e.class
    puts @err
  end
end

Then /^I should recieve a failed error$/ do
  #puts @hive_job_options.inspect
  lambda { @hive_job_options.options = @options }.should raise_error
end

Then /^error message should include "([^"]*)"$/ do |msg|
  lambda { @hive_job_options.options = @options }.should raise_error(msg)
end

Then /^I should generate options$/ do
  #gets called on initialize or set_options
end

Then /^"([^"]*)" should be added to the list$/ do |type|
  @hive_job_options.query_options.last.class.should == Jobs::QueryOption
end

Then /^The query value should be "([^"]*)"$/ do |query|
  @hive_job_options.query_options.last.query.should == query
end