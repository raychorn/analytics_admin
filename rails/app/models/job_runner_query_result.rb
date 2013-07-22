class JobRunnerQueryResult < ActiveRecord::Base
  belongs_to :job_runner
  belongs_to :query_result
end
