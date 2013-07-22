class CreateValueMappings < ActiveRecord::Migration
  def self.up
    create_table :value_mappings do |t|
      t.integer :data_parameter_id
      t.string :key
      t.string :value

      t.timestamps
    end
  end

  def self.down
    drop_table :value_mappings
  end
end
