class CreateDataResults < ActiveRecord::Migration
  def self.up
    create_table :data_results do |t|
      t.integer :query_result_id
      t.text :original_query_string
      t.text :data

      t.timestamps
    end
  end

  def self.down
    drop_table :data_results
  end
end
