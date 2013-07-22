class SampleResult < ActiveRecord::Base
end

namespace :app do
  desc <<-DESC
    Load sample data.
    Run using the command 'rake app:sample_data'
  DESC
  task :sample_data => [:environment] do
    # Only data not required in production should be here.
    # If it needs to be there in production, it belongs in seeds.rb.

    SampleResult.set_table_name 'sample_results_17'
    if SampleResult.table_exists?
      ActiveRecord::Migration.class_eval do
        drop_table :sample_results_17
      end
    end

    ActiveRecord::Migration.class_eval do
      create_table :sample_results_17 do |t|
        t.string :devicetype
        t.float :avg_upload_download
        t.datetime :server_date
      end
    end

    [
        [ "3GDevice", 12.2, "2010-12" ],
        [ "4GDevice", 10.2, "2011-01" ]
    ].each do |r|
      SampleResult.create(
          :devicetype => r[0],
          :avg_upload_download => r[1],
          :server_date => DateTime.strptime(r[2], '%Y-%m').to_time
      )
    end
  end
end
