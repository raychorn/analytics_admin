class ValueMapping < ActiveRecord::Base
  belongs_to :data_parameter

  validates_presence_of :key, :value
end
