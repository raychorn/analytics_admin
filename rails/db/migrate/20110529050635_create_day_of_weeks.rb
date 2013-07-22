class CreateDayOfWeeks < ActiveRecord::Migration
  def self.up
    create_table :day_of_weeks do |t|
      t.string :name
    end
  end

  def self.down
    drop_table :day_of_weeks
  end
end
