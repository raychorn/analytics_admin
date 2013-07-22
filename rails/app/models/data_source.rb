class DataSource < ActiveRecord::Base
  has_many :query_results
end
