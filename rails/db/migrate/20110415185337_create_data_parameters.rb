class CreateDataParameters < ActiveRecord::Migration
  def self.up
    create_table :data_parameters do |t|
      t.integer :group_id
      t.string :column_name
      t.string :symbolic_name
      t.string :display_name
      t.string :short_display_name
      t.text :description

      t.timestamps
    end
  end

  def self.down
    drop_table :data_parameters
  end
end
