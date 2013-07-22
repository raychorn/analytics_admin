class CreateJobRunnerQueryResults < ActiveRecord::Migration
  def self.up
    create_table :job_runner_query_results do |t|
      t.integer :job_runner_id
      t.integer :query_result_id

      t.timestamps
    end
  end

  def self.down
    drop_table :job_runner_query_results
  end
end
