class CreateAnalyticsSettings < ActiveRecord::Migration
  def self.up
    create_table :analytics_settings do |t|

      t.timestamps
    end
  end

  def self.down
    drop_table :analytics_settings
  end
end
