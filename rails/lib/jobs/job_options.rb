module Jobs
  module Exceptions
    class FailedJobError < StandardError; end
  end

  class ResultType
    JSON = 0
    MYSQL = 1
    TYPES = %w{JSON MYSQL}.freeze
    def self.all
      TYPES
    end
  end
  class QueryOption
    attr_accessor :query, :store_in_db
    attr_accessor :id
    def initialize(args)
      #puts "args: " + args.inspect
      begin
        @id = args["id"]
        @query = args["query_string"]
        @store_in_db = args["store_in_db"] ? args["store_in_db"] : false
        #puts "query: " + @query + " db: " + @store_in_db
      rescue => e
        raise Exceptions::FailedJobError.new("query option required parameter is missing (#{e}).")
      end
    end
    def table_name
        "results_" + self.id.to_s
    end
  end
  class HiveJobOptions
    # The +options+ attribute holds the raw options
    attr_accessor :options
    # The +query_options+ attribute holds a list of type
    # QueryOption which contains clean queries, table names, etc.
    attr_accessor :query_options
    attr_accessor :database
    attr_accessor :result_type

    def initialize(options={})
      @options = options
      @query_options = []
      @database = "default"
      @result_type = ResultType::MYSQL #other types: ResultType::JSON
      unless options.nil? || options.empty?
        generate_query_options(options)
      else
        puts "options not generated yet because options is empty"
      end
      raise ArgumentError.new("options is nil") if options.nil?
    end

    # The +options+ method gives options a value
    # === Parameters
    # * options hash {"database_name" => "", "query_results" => {}}
    # === Returns
    # options
    def options=(options)
      @options = options
      #puts "Options: " + options.inspect
      generate_query_options(options)
    end

    # The +parent+ method returns the first
    # (parent) of the query_options list
    def parent
      self.query_options.first
    end

    protected

    def generate_query_options(options)
      begin
        raise "options with 0 queries" if options["query_results"].empty?
        self.database = options["database_name"].nil? ? @database : options["database_name"]
        self.result_type = options["result_type"].nil? ? @result_type : options["result_type"]
        options["query_results"].each do |qr|
          self.query_options << QueryOption.new(qr["query_result"])
        end
      rescue => e
        raise Exceptions::FailedJobError.new("options error: #{e}")
      ensure
        set_query_options_variables
      end
    end

    # The +set_query_options_variables+ method parses the query string
    # and replaces the variables/keywords in the query with it's counterparts
    # === Parameters
    # * none (self.query_options)
    # === Variables/Keywords
    # * #PARENT, #HIVE_PARENT_TABLE, #PARENT_TABLE
    # * #PREVIOUS, #PREVIOUS_TABLE
    # * #DATE_TODAY, #DATE_YESTERDAY
    # === TODO: Do we want to have the keyword/targets in a model so we can define/add more at anytime?
    # === TODO: #DATE keywords
    # === TODO: Check for ';' and remove them if necessary
    # === TODO: Add #PREVIOUS_MONTH, #CURRENT_MONTH, #TODAY, #YESTERDAY
    # === TODO: Add more validation for previous and child
    # === NOTE: Watch out for Repeating Keywords (should not have: #PARENT & #PARENT_TABLE)
    def set_query_options_variables
      begin
        parent_keywords = ["#PARENT", "#HIVE_PARENT_TABLE", "#TABLE_PARENT"]
        parent_match = Regexp.new( '(' + parent_keywords.join("|") + ')' )
        previous_keywords = ["#PREVIOUS", "#TABLE_PREVIOUS"]
        previous_match = Regexp.new( '(' + previous_keywords.join("|") + ')' )
        child_match = Regexp.new( '(' + "#CHILD_\\d+" + ')' )
        self.query_options.each_with_index do |qo, i|
          ##Parent Matches
          str = qo.query.gsub!(parent_match, parent.table_name)
          #If there was a substitution/match
          unless str.nil?
            #Check that the parent is being stored in hive and not the result db.
            #raise Exceptions::FailedJobError.new("using parent keyword, when parent is not queued to be stored in hive") if parent.store_in_db
            raise Exceptions::FailedJobError.new("parent cannot be a parent of itself, do not reference parent keyword") if qo.id == parent.id
          end
          ##Previous Matches
          str = qo.query.gsub!(previous_match, self.query_options[i-1].table_name)
          str = qo.query.gsub!(child_match) { |child| self.query_options[child[/\d+/].to_i].table_name }
        end
      rescue => e
        raise Exceptions::FailedJobError.new("options error: #{e}")
      end
    end
  end
end