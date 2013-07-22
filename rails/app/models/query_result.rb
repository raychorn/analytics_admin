class QueryResult < ActiveRecord::Base
  # by default only the most recent result is returned
  has_many :latest_data_results, :class_name => "DataResult", :limit => 1, :order => 'updated_at DESC'
  has_many :job_runner_query_results, :dependent => :destroy
  has_many :job_runners, :through => :job_runner_query_results
  belongs_to :data_source

  attr_accessor :dynamic_table_name, :sample_option, :sql_string, :result_type, :result_set, :query_result_id,
    :database_name

  # TODO should validate two query_results don't have the same query

  # option for where to get data from: use real data, load from the portal, or load from the web service
  class Sample
      REAL = 0 # default
      REAL_JSON = 1
      PORTAL = 2
      REMOTE = 3
      PORTAL_JSON = 4
      REMOTE_JSON = 5

      def self.all
        ALL
      end

      def self.options
        OPTIONS
      end

      def self.result_is_json?(sample_option)
        case sample_option
          when Sample::REAL_JSON
            true
          when Sample::PORTAL_JSON
            true
          when Sample::REMOTE_JSON
            true
          else
            false
        end
      end

      def self.real_data?(sample_option)
        case sample_option
          when nil
            true
          when Sample::REAL
            true
          when Sample::REAL_JSON
            true
          else
            false
        end
      end

      private #################

      ALL = [REAL, REAL_JSON, PORTAL, REMOTE, PORTAL_JSON, REMOTE_JSON]

      OPTIONS = [
          [REAL, 'real data'],
          [REAL_JSON, 'real json data'],
          [PORTAL, 'portal sample'],
          [REMOTE, 'remote sample'],
          [PORTAL_JSON, 'portal JSON sample'],
          [REMOTE_JSON, 'remote JSON sample']
      ]
  end

  def database_name=(database_name)
    @database_name = database_name
    self.data_source = DataSource.find_or_initialize_by_database_name(self.database_name)
  end

  def result_set
    if Sample.result_is_json?(self.sample_option)
      ResultSet.new(
          :result_type => 'json',
          :latest_data_results => self.latest_data_results
      )
    else
      if self.sql_string.present?
        final_sql_string = self.sql_string.gsub(/\{table_name\}/, self.dynamic_table_name)
      else
        final_sql_string = self.sql_string
      end

      ResultSet.new(
          :result_type => 'active_record',
          :dynamic_table_klass => self.dynamic_table_klass,
          :final_sql_string => final_sql_string
      )
    end
  end

  def dynamic_table_name
    if self.sample_option == Sample::REMOTE
      self.dynamic_table_name = "sample_results_#{self.id}"
    else
      self.dynamic_table_name = "results_#{self.id}"
    end
  end

  def setup_associations
    self.data_source = DataSource.find_or_initialize_by_database_name(self.database_name)
  end

protected #####################

  def dynamic_table_klass
    if !defined?(@klass) || @klass.nil?
      # TODO fix warning about "already initialized constant"
  #    if defined?(dynamic_table_name.camelize.constantize)
  #      klass = dynamic_table_name.camelize.constantize
  #    else
      @klass = Object.const_set(self.dynamic_table_name.camelize, Class.new(ActiveRecord::Base))
      @klass.set_table_name(self.dynamic_table_name)
      @klass
  #    end
    else
      @klass
    end
  end

end
