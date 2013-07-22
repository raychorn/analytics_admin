#--
# Job Runner Rspec Descriptions.
#--
require 'spec_helper'

module JobRunnerSpecHelpers
  def job_uuid
    @job_uuid ||= nil
  end
  def job_uuid=(uuid)
    @job_uuid = uuid
  end
end

describe JobRunner do
  include JobRunnerSpecHelpers
  before(:each) do
    @job_runner = JobRunner.new(
        :database_name => "localhost",
        :queries => [QueryResult.new]
    )
  end

  it "is valid with valid attributes" do
    @job_runner.should be_valid
  end

  it "is not vaid without database_name" do
    @job_runner.database_name = nil
    @job_runner.should_not be_valid
  end

  describe "#start" do
    it "has at least one QueryResult in queries attribute" do
      @job_runner.queries.first.should_not be_nil
      @job_runner.queries.first.class.should == QueryResult
    end
    context "when there is no QueryResult in queries attribute" do
      it "does not set job_uuid" do
        @job_runner.queries = []
        @job_runner.save!
        @job_uuid = @job_runner.start
        @job_uuid.should be_nil
      end
      it "keeps status of idle" do
        @job_runner.status[:status].should == "idle"
      end
    end
    context "when a job with valid queries is started" do
      it "queues a HiveJob with the database and query_results" do
        @job_runner.queries = [QueryResult.new(:query_string => "select * from testHiveDriverTable",:store_in_db => 1)]
        @job_runner.save!
        @job_runner.start
        @job_runner.status["status"].should == ("queued" or "working" or "completed" or "running")
        @job_runner.status["status"].should_not == ("idle" or "killed")
      end
      it "sets the JobRunner job_uuid with a unique identifier" do
        @job_runner.queries = [QueryResult.new(:query_string => "select * from testHiveDriverTable",:store_in_db => 1)]
        @job_runner.save!
        @job_runner.start
        job_uuid=@job_runner.job_uuid
        JobRunner.find_all_by_job_uuid(job_uuid).count.should equal 1
      end
      context "when the query_result has invalid queries" do

      end
    end
  end

end