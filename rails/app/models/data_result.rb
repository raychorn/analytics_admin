class DataResult < ActiveRecord::Base
  belongs_to :query_result
  has_one :data_source, :through => :query_result
end
