#--
# Hive Server Rspec Descriptions.
#--
require 'java'
require 'spec_helper'
require 'hive/hive_server'

java_import com.google.gson.Gson
java_import com.google.gson.JsonObject

module HiveSpecHelpers
  def connect_valid_server(database='default')
    hive_manager = nil
    HiveServer::connect('hivedev1','10000',database) do |connection|
      hive_manager = connection
    end
  end
end

describe HiveServer do
  include HiveSpecHelpers
  describe "#connect" do
    context "with valid values and server reachable" do
      it "should return a hive server manager class" do
        connection = connect_valid_server
        connection.class.should == HiveServer::Manager
      end
      it "should return online equals true" do
        connection = connect_valid_server
        connection.connect
        connection.online?.should == true
      end
    end
    context "with server reachable but non-existing database" do
      it "should return online equals false" do
        connection = connect_valid_server('doesnotexist')
        connection.connect
        connection.online?.should == false
      end
    end
  end

  describe "execute queries" do
    context "single valid query expecting json result set" do
      it "sends query to hive server with no error" do
        connection = connect_valid_server
        connection.connect
        lambda { connection.dao.executeQuery('select key,value from testHiveDriverTable')}.should_not raise_error
        #JSON.load(result.to_s).is_a?(JSON).should_not raise_error
      end
      it "expects a result as JSON" do
        connection = connect_valid_server
        connection.connect
        result = connection.dao.executeQuery('select key,value from testHiveDriverTable')
        JSON.load(result.toString)["columns"].should_not be_nil
      end
    end
    context "single valid query not expecting result set" do
      it "query does not return result or error" do
        pending
      end
    end
  end
end