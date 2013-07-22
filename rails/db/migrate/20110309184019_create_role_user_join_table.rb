class CreateRoleUserJoinTable < ActiveRecord::Migration
  def self.up
    create_table :roles_users, :id => false do |t|
      t.references :role, :user
    end
  end

  def self.down
  end
end
