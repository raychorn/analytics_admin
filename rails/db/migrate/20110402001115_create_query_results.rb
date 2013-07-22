class CreateQueryResults < ActiveRecord::Migration
  def self.up
    create_table :query_results do |t|
      t.integer :data_source_id
      t.text :query_string
      t.boolean :store_in_db

      t.timestamps
    end
  end

  def self.down
    drop_table :query_results
  end
end
