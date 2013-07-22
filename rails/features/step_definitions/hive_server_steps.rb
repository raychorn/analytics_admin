module HiveServerStepsHelpers
end

# -- hive_server_create_connection
Given /^I am not yet connected to Hive Server$/ do
end

When /^I connect to Hive Server with parameters "([^"]*)","([^"]*)","([^"]*)"$/ do |server, port, database|
  @connection = nil
  HiveServer.connect(server,port,database) do |connection|
    @connection = connection
  end
end

When /^I connect to Hive Server with parameters "([^"]*)"$/ do |server|
  @connection = nil
  HiveServer.connect(server) do |connection|
    @connection = connection
  end
end

Then /^I should recieve "([^"]*)" on connection manager online$/ do |status|
  @connection.online?.to_s.should == status
end

Then /^I should recieve a connection error$/ do
  @connection.should raise_error
end


# -- hive_server_execute_query
Given /^an active "([^"]*)" connection manager$/ do |status|
  @connection = nil
  HiveServer.connect('hivedev1') do |connection|
    connection.online?.to_s.should == status
    @connection = connection
  end
end

When /^I execute the query "([^"]*)"$/ do |query|
  @connection.open
  @result = @connection.dao.executeQuery(query)
end

When /^expect the result in "([^"]*)"$/ do |type|
  if type == "JSON"
    #Checking Object type, later might be better to load it and check format
    @result.class.should == Java::OrgJsonSimple::JSONObject
  end
end

Then /^I should recieve the result as "([^"]*)"$/ do |type|
  if type == "JSON"
    #Checking Object type, later might be better to load it and check format
    @result.class.should == Java::OrgJsonSimple::JSONObject
  end
end

Then /^I should recieve a json result with the following json members:$/ do |members_table|
  members_table.hashes.each do | hash |
    json = JSON.load(@result.toString)
    json["#{hash["members"]}"].should_not be_nil
  end
end
