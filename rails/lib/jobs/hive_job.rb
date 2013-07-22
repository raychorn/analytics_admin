require 'hive/hive_server'
require 'jobs/job_options'
module Jobs

  class HiveJob < Resque::JobWithStatus
    @queue = :default
    attr_reader :result
    attr_reader :job_options

    # needed by resque-scheduler
    def self.scheduled(queue, klass, *args)
      job_runner_id = args.try(:first).try(:fetch, 'job_runner_id')
      puts "about to run job runner with id = #{job_runner_id}"
      if job_runner_id
        job_runner = JobRunner.find(job_runner_id) # could get ActiveRecord::RecordNotFound
        job_uuid = create(*args)
        job_runner.job_uuid = job_uuid
        job_runner.save!
      else
        create(*args)
      end
    end

    def perform
      # requires both :database_name => string and :query_results => array of QueryResults
      # these can be accessed here via the options hash:
      #     options['database_name']
      #     options['query_results']

      # the format of the query_results JSON array follows the QueryResult model, e.g.:
      # [
      #    {
      #        "query_result": {
      #            "created_at": "2011-04-21T21: 16: 31Z",
      #            "data_source_id": 2,
      #            "id": 3,
      #            "query_string": "select * from foo...",
      #            "store_in_db": true,
      #            "updated_at": "2011-04-21T21: 16: 31Z"
      #        }
      #    },
      #    { "query_result": "..."
      # ]

      # for each of the data_results where store_in_db == true, store the result in query_result.data_results

      puts "options: " + options.inspect
      @job_options = HiveJobOptions.new(options)
      unless options['query_results'].empty?
        HiveServer.run(@job_options)
        #@result = query_perform options
        #completed('result_count' => '1')
        return
      end
      # Options Error
      failed('result_count' => '0')
      raise "Options Query Results was empty."
    end

    def on_success
      #puts "RESULT is: " + @result.inspect
      #For testing: Create a DataResult Object and Save it
      #begin
      #  res = DataResult.new(:query_result_id => options["query_results"].first["query_result"]["id"],
      #                   :original_query_string => options["query_results"].first["query_result"]["query_string"],
      #                   :data => @result
      #  )
      #  res.save!
      #rescue ActiveRecord::RecordInvalid
      #  raise "something went wrong trying to save result!"
      #end
    end
    def on_failure(e)
      puts "Failure: The Job with name:#{name} failed with: #{e}"
    end

    # this is temporarily
    def query_perform(options)
      query = options["query_results"].first["query_result"]["query_string"]
      database = "default" #options["database_name"]
      #TODO: Use Options for connection parameters
      HiveServer.connect('hivedev1','10000',database) do |db|
        puts "Is Hive Server online?: " + db.online?.to_s
        if db.online?
          result = db.dao.executeQuery(query)
          json = JSON.load(result.to_s)
        end
      end
    end

    # this method has the same requirements as HiveJob.perform
    def dummy_perform
      puts "###db_name=#{options['database_name'].inspect}"
      puts "###options=#{options['query_results'].inspect}"
      sleep 30
    end
  end
end