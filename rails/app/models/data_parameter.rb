class DataParameter < ActiveRecord::Base
  has_many :value_mappings, :dependent => :destroy
end
