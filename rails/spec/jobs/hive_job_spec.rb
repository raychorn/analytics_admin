#--
# Hive Job Rspec Descriptions.
#--
require 'spec_helper'
require 'support/helpers/hive_job_spec_helper'

describe Jobs::HiveJob do
  include HiveJobSpecHelpers

  describe Jobs::HiveJobOptions do
    describe "#initialize" do
      context "with options being nil" do
        it "should raise an argument error" do
          lambda { Jobs::HiveJobOptions.new(nil) }.should raise_error(ArgumentError,"options is nil")
        end
      end
      context "with options hash including 0 queries" do
        it "should raise a failed error" do
          options = HiveJobSpecHelpers.create_run_options(0)
          puts options.inspect
          lambda { Jobs::HiveJobOptions.new(options) }.should raise_error(Jobs::Exceptions::FailedJobError,"options error: options with 0 queries")
        end
      end
      context "with at least 1 query in options" do
        it "should not raise a failure during initialization" do
          options = HiveJobSpecHelpers.create_run_options(1)
          puts options.inspect
          lambda { Jobs::HiveJobOptions.new(options) }.should_not raise_error(Jobs::Exceptions::FailedJobError,"options error: options with 0 queries")
        end
        it "should generate a query option and add it to the list" do
          options = HiveJobSpecHelpers.create_run_options(1)
          puts options.inspect
          @job_hive_option = Jobs::HiveJobOptions.new(options)
          @job_hive_option.query_options.last.class.should == Jobs::QueryOption
        end
      end
    end
    describe "#options=" do
      context "after initialize by setting the options attribute" do
      end
    end
  end

end