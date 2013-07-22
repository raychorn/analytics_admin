class CreateJobRunners < ActiveRecord::Migration
  def self.up
    create_table :job_runners do |t|
      t.datetime :last_run

      t.timestamps
    end
  end

  def self.down
    drop_table :job_runners
  end
end
