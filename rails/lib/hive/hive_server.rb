require 'java'

java_package 'org.smithmicro.analytics.hive'

java_import org.smithmicro.analytics.hive.server.HiveDAOFactory
java_import org.smithmicro.analytics.hive.server.HiveDAOFactory
java_import org.smithmicro.analytics.hive.server.HiveAdminDAO
java_import org.smithmicro.analytics.hive.server.HiveDAOException
java_import java.sql.Connection

module HiveServer

  @rails_env = Rails.configuration.database_configuration[Rails.env]
  def self.rails_env
    @rails_env
  end

  #TODO: server should either be part of datasource or it should be a global setting that changes per environment
  def self.connect(server, port='10_000', database='default', dao_type=HiveDAOFactory::HiveDAOType::ADMIN)
    hive_manager = Manager.new(server,port,database,dao_type)
    ret = nil
    begin
      hive_manager.connect
      ret = yield(hive_manager)
    ensure
      hive_manager.disconnect
      ret
    end
  end

  # The +run+ class method will take a HiveJobOptions class
  # and connect to the server and run each query accordingly
  # === Parameters
  # * Jobs::HiveJobOptions
  # === #TODO: Don't hardcode Server
  def self.run(hive_job_options)
    database = hive_job_options.database
    connect(@rails_env["remote_hive_server"].to_s,'10000',database) do |manager|
      if manager.online?
        hive_job_options.query_options.each { |qo|
          manager.run(qo,hive_job_options.result_type)
        }
      end
    end
  end

  class Manager

    attr_reader :server, :port, :database
    attr_reader :dao

    def initialize(server, port='10_000', database='default',dao_type=HiveDAOFactory::HiveDAOType::ADMIN)
      @logger = org.apache.log4j.Logger.getLogger("org.smithmicro.analytics.hive.#{__FILE__}")
      @server, @port, @database = server, port, database
      @dao_type = dao_type
      #connection = HiveConnectionManager.connect(@server,@port)
    end

    def connect
      begin
        @logger.info("HiveServer?online: getting HiveDAO...")
        @dao = HiveDAOFactory.getHiveDAO(@server,@port,@database,@dao_type)
        @logger.info("HiveServer?online: got HiveDAO.")
        true
      rescue HiveDAOException => e
        @logger.error(e.message)
        @logger.error(e.backtrace.inspect)
      rescue Exception => e
        @logger.error(e.message)
        @logger.error(e.backtrace.inspect)
        false
      end
    end
    alias :open :connect

    def disconnect
      @dao.closeConnection if @dao
    end
    alias :close :disconnect

    # The +online?+ class method checks if Hive Service
    # is running before trying to create an instance.
    # === Parameters
    # * none
    # === Example
    #  Manager.online?
    # === Returns
    # * true if online
    # * false if offline
    def online?
      unless @dao.nil?
        return true
      end
      false
    end

    # The +run+ method will take a QueryOption class
    # and run query accordingly
    # === Parameters
    # * Jobs::QueryOption
    def run(query_option, result_type=Jobs::ResultType::MYSQL)
      #Then we want the result persisted to the app's db
      if query_option.store_in_db
          save_result(query_option, result_type)
      else
      #Then we want the result to be persisted to hive's db
          save_result_to_hive(query_option)
      end
    end

    def status
      puts "Hive Server Status:"
      puts "Online" if Manager.online?
    end

    def hive_type_to_rails_migration_symbol(type)
      case type
        when "int"
          "integer"
        when "double"
          "float"
        else
          type
      end
    end

    def escape_mysql_keyword(key)
      res = key.to_s
      hive_keywords = ["key", "date"]
      hive_match = Regexp.new( '(' + hive_keywords.join("|") + ')' )
      #str = res.gsub!(hive_match) { |match| res = "`" + match + "`" }
      str = res.gsub(hive_match) { |match|
        if res.eql?(match)
          res = "`" + match + "`"
        end
      }
      res
    end

    #def createTable
    #  connection.createTable("Create table test (col_1 STRING)")
    #end

    protected

    # The +save_result_as_+ result methods will persist the result into
    # the application's own database as the format desired.
    def save_result(query_option,result_type)
      case result_type
        when Jobs::ResultType::JSON
          save_result_as_json(query_option)
        when Jobs::ResultType::MYSQL
          save_result_to_mysql(query_option)
      end
    end

    def save_result_as_json(query_option)
      result = @dao.execute_query(query_option.query)
      json = JSON.load(result.toString)
      puts json.inspect
      begin
        res = DataResult.new(:query_result_id => query_option.id,
                         :original_query_string => query_option.query,
                         :data => json.inspect
        )
        res.save!
      rescue ActiveRecord::RecordInvalid
        raise "something went wrong trying to save result!"
      end
    end

    # The +save_result_to_+ result methods will persist the results into
    # the database specified in the function. In the default format.
    def save_result_to_hive(query_option)
      #TODO: Don't just drop table, should use insert into partition
      @dao.drop_table(query_option.table_name)
      @dao.store_query_output_to_hive_table(query_option.query,query_option.table_name)
    end

    # Currently the data must be offloaded to hive first then it can
    # be loaded into mysql.
    def save_result_to_mysql(query_option)
      @dao.drop_table(query_option.table_name)
      @dao.store_query_output_to_hive_table(query_option.query,query_option.table_name)
      #TODO: Use mysql connection manager instead of creating table in rails, we'll need more functionality later
      #TODO: MySQL Connection Manager in Java using JDBC so it mimics current Hive's functionality
      #TODO: Currently Hive Server needs visibility to MySQL, this won't usually work on dev, unless you have a VM running hive
      table_desc_json = JSON.load(@dao.describe_table(query_option.table_name))
      columns = Array.[]
      unless table_desc_json.nil?
        #Drop Table
        if ActiveRecord::Base.connection.tables.include?(query_option.table_name)
          ActiveRecord::Migration.class_eval { drop_table query_option.table_name }
        end
        #Create Table
        ActiveRecord::Migration.class_eval { create_table query_option.table_name }
        #Traverse each Data Column to get Col_Name and Col_Type and add columns to mysql
        table_desc_json["data"].each do |row|
          column_name = row.first.to_sym
          columns << escape_mysql_keyword(column_name)
          type = hive_type_to_rails_migration_symbol(row.second).to_sym
          ActiveRecord::Migration.class_eval { add_column query_option.table_name, column_name, type }
        end
      end
      #TODO: Pass values from Rails Env, Replace this method with connection manager
      ip = HiveServer.rails_env["remote_mysql_host"] ? HiveServer.rails_env["remote_mysql_host"] : Socket::getaddrinfo(Socket.gethostname,"echo",Socket::AF_INET)[0][3]
      port = HiveServer.rails_env["remote_mysql_port"] ? HiveServer.rails_env["remote_mysql_port"] : "3306"
      @dao.export_query_output_to_my_sql_table(ip.to_s,port.to_s,HiveServer.rails_env["database"].to_s,HiveServer.rails_env["username"].to_s,HiveServer.rails_env["password"].to_s,query_option.table_name,columns.to_java(:string))
    end


  end

  class ResultSet < Array

    def to_json(out_file=nil)
    end

  end

  class StdOutLogger
    %w(fatal error warn info debug).each do |level|
      define_method level.to_sym do |message|
        STDOUT.puts(message)
     end
   end
  end

end
