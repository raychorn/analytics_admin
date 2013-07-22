class AddJobUuidToJobRunners < ActiveRecord::Migration
  def self.up
    add_column :job_runners, :job_uuid, :string
  end

  def self.down
    remove_column :job_runners, :job_uuid
  end
end
