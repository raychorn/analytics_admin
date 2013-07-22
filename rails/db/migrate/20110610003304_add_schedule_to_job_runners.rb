class AddScheduleToJobRunners < ActiveRecord::Migration
  def self.up
    add_column :job_runners, :schedule, :string
  end

  def self.down
    remove_column :job_runners, :schedule
  end
end
