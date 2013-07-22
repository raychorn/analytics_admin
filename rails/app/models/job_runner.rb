class JobRunner < ActiveRecord::Base
  has_many :job_runner_query_results, :dependent => :destroy
  has_many :query_results, :through => :job_runner_query_results
  belongs_to :data_source

  attr_accessor :status, :database_name
  attr_accessor :queries # array transient queries to accept as input, same objects as QueryResult

  after_initialize :init

  def status=(status)
    @status = status
    write_attribute(:status, status)
  end

  def status
    self.retrieve_status
    @status
  end

  def start
    # note the arguments for create are JSON encoded, so verify the data is valid
    if self.query_results.present?
      self.job_uuid = Jobs::HiveJob.create(:database_name => self.data_source.database_name,
                                           :query_results => self.query_results)
      self.save
    end
  end

  def enable_schedule
    raise 'save the job_runner before enabling the schedule' if self.new_record?
    schedule_options =
        {
            'cron' => self.schedule,
            'custom_job_class' => 'Jobs::HiveJob',
            'queue' => 'default',
            'args' => {
                :database_name => self.database_name,
                :query_results => self.query_results,
                :job_runner_id => self.id
            },
            'description' => "Jobs::HiveJob - job_runner id# #{self.id}"
        }
    Resque.set_schedule("job_runner_#{self.id}", schedule_options)
  end

  def disable_schedule
    raise 'save the job_runner before enabling the schedule' if self.new_record?
    Resque.remove_schedule("job_runner_#{self.id}")
  end

  def setup_associations
    self.data_source = DataSource.find_or_initialize_by_database_name(self.database_name)
    # only write to query_results if queries is set (e.g. if queries = [], this still runs, but not if query = nil)
    if self.queries.present?
      jr_query_results = []
      self.queries.each do |query|
        # TODO find the matching query_result, if it exists use that, else create a new one
#        qr = QueryResult.where(:query_string => query['query_string'], :data_source_id => self.data_source_id).first
#        qr = QueryResult.where(:id => query['query_result_id']).first
#        qr ||= QueryResult.new
#        qr.data_source_id = self.data_source_id
#        qr.query_string = query['query_string']
#        qr.store_in_db = query['store_in_db']
        qr = QueryResult.find(query['query_result_id'])
        jr_query_results << qr
      end
      self.query_results = jr_query_results
    end
    return true
  end

protected #########################

  def init
    self.retrieve_status
  end

  def retrieve_status
    # job_uuid must be set by the job class (e.g. HiveJob)
    stat = Resque::Status.get(self.job_uuid)
    if stat.blank?
      stat = {:status => 'idle', :message => 'Never run'}
    end
    self.status = stat
  end

end
