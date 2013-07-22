# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 20110610003304) do

  create_table "analytics_settings", :force => true do |t|
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "data_parameters", :force => true do |t|
    t.integer  "group_id"
    t.string   "column_name"
    t.string   "symbolic_name"
    t.string   "display_name"
    t.string   "short_display_name"
    t.text     "description"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "data_results", :force => true do |t|
    t.integer  "query_result_id"
    t.text     "original_query_string"
    t.text     "data"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "data_sources", :force => true do |t|
    t.string   "database_name"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "day_of_weeks", :force => true do |t|
    t.string "name"
  end

  create_table "groups", :force => true do |t|
    t.datetime "created_at"
    t.datetime "updated_at"
    t.string   "name"
  end

  create_table "job_runner_query_results", :force => true do |t|
    t.integer  "job_runner_id"
    t.integer  "query_result_id"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "job_runners", :force => true do |t|
    t.datetime "last_run"
    t.datetime "created_at"
    t.datetime "updated_at"
    t.string   "job_uuid"
    t.integer  "data_source_id"
    t.string   "schedule"
  end

  create_table "permissions", :force => true do |t|
    t.string   "name"
    t.string   "description"
    t.integer  "ability"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "permissions_roles", :id => false, :force => true do |t|
    t.integer "permission_id"
    t.integer "role_id"
  end

  create_table "query_results", :force => true do |t|
    t.integer  "data_source_id"
    t.text     "query_string"
    t.boolean  "store_in_db"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "roles", :force => true do |t|
    t.string   "name"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "roles_users", :id => false, :force => true do |t|
    t.integer "role_id"
    t.integer "user_id"
  end

  create_table "user_settings", :force => true do |t|
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  create_table "users", :force => true do |t|
    t.string   "first_name"
    t.string   "last_name"
    t.string   "email",                               :default => "", :null => false
    t.string   "encrypted_password",   :limit => 128, :default => "", :null => false
    t.string   "reset_password_token"
    t.string   "remember_token"
    t.datetime "remember_created_at"
    t.integer  "sign_in_count",                       :default => 0
    t.datetime "current_sign_in_at"
    t.datetime "last_sign_in_at"
    t.string   "current_sign_in_ip"
    t.string   "last_sign_in_ip"
    t.string   "authentication_token"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

  add_index "users", ["email"], :name => "index_users_on_email", :unique => true
  add_index "users", ["reset_password_token"], :name => "index_users_on_reset_password_token", :unique => true

  create_table "value_mappings", :force => true do |t|
    t.integer  "data_parameter_id"
    t.string   "key"
    t.string   "value"
    t.datetime "created_at"
    t.datetime "updated_at"
  end

end
