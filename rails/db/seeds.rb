# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rake db:seed (or created alongside the db with db:setup).
#
# Examples:
#
#   cities = City.create([{ :name => 'Chicago' }, { :name => 'Copenhagen' }])
#   Mayor.create(:name => 'Daley', :city => cities.first)

# create or update permissions
[[1, "Create Job", ""],
 [2, "Configure User Settings", ""]].each do |p_array|
  permission = Permission.find_by_ability(p_array[0])
  permission ||= Permission.new
  permission.attributes = {
    :ability => p_array[0],
    :name => p_array[1],
    :description => p_array[2]
  }
  permission.save!
end

# create "administrator" role
admin_role_name = 'administrator'
admin_role = Role.find_by_name(admin_role_name)
admin_role ||= Role.new
admin_role.attributes = {
  :name => admin_role_name
}
admin_role.permissions = Permission.all
admin_role.save!

# create or update admin user
admin_user_email = 'admin@analytics.local'
admin_user = User.find_by_email(admin_user_email)
admin_user ||= User.new
admin_user.attributes = {
  :email => admin_user_email,
  :password => 'analytics',
  :password_confirmation => 'analytics',
  :first_name => 'admin',
  :last_name => 'admin'
}
admin_user.add_role(admin_role)
admin_user.save!

[[1, "Monday"],
 [2, "Tuesday"],
 [3, "Wednesday"],
 [4, "Thursday"],
 [5, "Friday"],
 [6, "Saturday"],
 [7, "Sunday"]].each do |d_array|
  day = DayOfWeek.where(:id => d_array[0]).first
  day ||= DayOfWeek.new
  day.attributes = {
    :name => d_array[1]
  }
  day.save!
end
