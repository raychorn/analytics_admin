require 'csv'

class ActiveRecord::Base
  # http://railspikes.com/2008/2/1/loading-seed-data
  # http://blogwi.se/post/47083009/slightly-pimped-activerecord-create-or-update
  # http://www.mbleigh.com/2008/2/19/activerecord-base-create_or_update-on-steroids
  # given a hash of attributes including the ID, look up the record by ID.
  # If it does not exist, it is created with the rest of the options.
  # If it exists, it is updated with the given options.
  #
  # Raises an exception if the record is invalid to ensure seed data is loaded correctly.
  #
  # Returns the record.
  def self.create_or_update_by(attr_sym, attr_value, options = {})
#    id = options.delete(:id)

    record = where(attr_sym => attr_value).first || new(attr_sym => attr_value)
    record.attributes = options
    record.save!
    record
  end
end

module SeedDataHelper
  def self.create_or_update_objects(class_type, seed_fields, seeds_hash)
    puts "seeding #{class_type}..."
    objects_hash = {}
    seeds_hash.each do |obj_key, obj_attributes|
      options = {}
      seed_fields.each_index do |i|
        options[seed_fields[i]] = obj_attributes[i]
      end
      objects_hash[obj_key] = class_type.send(:create_or_update_by, seed_fields[0], obj_attributes[0], options)
    end
    objects_hash
  end

  def self.load_data_params_from_csv(group, file_path)
    csv_file = File.new(file_path, 'r')
    # skip header
    csv_file.gets
    while (line = csv_file.gets)
      row = CSV.parse_line(line)
      d_param = DataParameter.new(
          :group_id => group.id,
#          :integer_key => row[0],
          :column_name => row[1],
          :symbolic_name => row[2],
          :display_name => row[3],
          :short_display_name => row[4],
          :description => row[5]
      )
      if row[6].blank?
        d_param.value_mappings = []
      else
        value_mappings = JSON.parse(row[6]).collect{|k, v| ValueMapping.new(:key => k, :value => v)}
        d_param.value_mappings = value_mappings
      end
      d_param.save!
    end
  end
end
