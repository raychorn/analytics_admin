class AddDataSourceIdToJobRunner < ActiveRecord::Migration
  def self.up
    add_column :job_runners, :data_source_id, :integer
  end

  def self.down
    remove_column :job_runners, :data_source_id
  end
end
