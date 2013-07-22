class JobStatus
  attr_accessor :id, :name

  def initialize(id, name)
    @id = id
    @name = name
  end

  RUNNING = JobStatus.new(0, 'running')
  QUEUED = JobStatus.new(0, 'queued')
  SUCCESS = JobStatus.new(0, 'success')
  ERROR = JobStatus.new(0, 'error')
  FAILURE = JobStatus.new(0, 'failure')
  STATUSES = [RUNNING, QUEUED, SUCCESS, ERROR, FAILURE]
    
  def self.all
    STATUSES
  end
  
end