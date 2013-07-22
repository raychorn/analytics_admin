require 'analytics/worker_groups'
require 'analytics/helpers'
require 'analytics/helpers/daemon_spawn'
#TODO: Use @logger instead of puts
module Analytics
  class Worker
    #No need to persist workers as modules, should be treated as separate processes
    #so that externally they can be monitored. This is a utility class that can be
    #used programmatically, but should be initiated by Java/Rake from Outside.

    attr_reader :queues, :interval, :pid

    @logger = org.apache.log4j.Logger.getLogger("org.smithmicro.analytics.analytics.#{__FILE__}")

    def initialize(queues, interval = 5, verbose = 0)
      @queues = queues
      @interval = interval
      @vervose = verbose
    end

    def start
      unless Helpers.is_windows? #Windows not Supported
        rake_file = Rails.root.to_s+"/Rakefile"
        java_path, rake_path = Helpers.which('java'), Helpers.which('rake')
        jruby_jar = Helpers.which_jar('jruby-complete-1.6.1.jar')
        puts "Java: " + java_path.to_s
        puts "Rake: " + rake_path.to_s
        puts "Jruby: " + jruby_jar.to_s
        if !java_path.nil? and !rake_path.nil? and !jruby_jar.nil?
          class_path = java.lang.System.getProperties['java.class.path'].to_s + ":" + Dir[Rails.root.to_s+"/lib/*.jar"].join(':')
          java_args = '-Xmx384m'
          boot_path = "-Xbootclasspath/a:#{jruby_jar}"
          jruby_main = "org.jruby.Main"
          resque_cmd = "resque:work"
          queue = "QUEUE=#{@queues}"
          #@pid = DaemonSpawn.spawnp '/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/bin/java', '-cp', '/Users/lramos/Development/SVN/analytics/trunk/analytics_admin/rails/target/test-classes:/Users/lramos/Development/SVN/analytics/trunk/analytics_admin/rails/target/classes:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive/1.1/hive-1.1.jar:/Users/lramos/.m2/repository/cocoon/cocoon-ajax/2.1.8/cocoon-ajax-2.1.8.jar:/Users/lramos/.m2/repository/javax/jms/jms/1.1/jms-1.1.jar:/Users/lramos/.m2/repository/log4j/log4j/1.2.15/log4j-1.2.15.jar:/Users/lramos/.m2/repository/javax/mail/mail/1.4/mail-1.4.jar:/Users/lramos/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:/Users/lramos/.m2/repository/com/sun/jdmk/jmxtools/1.2.1/jmxtools-1.2.1.jar:/Users/lramos/.m2/repository/com/sun/jmx/jmxri/1.2.1/jmxri-1.2.1.jar:/Users/lramos/.m2/repository/commons-logging/commons-logging/1.0.4/commons-logging-1.0.4.jar:/Users/lramos/.m2/repository/commons-logging/commons-logging-api/1.0.4/commons-logging-api-1.0.4.jar:/Users/lramos/.m2/repository/com/google/code/gson/gson/1.7.1/gson-1.7.1.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hadoop-core/0.20-append/hadoop-core-0.20-append.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive-exec/0.6.0/hive-exec-0.6.0.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive-jdbc/0.6.0/hive-jdbc-0.6.0.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive-service/0.6.0/hive-service-0.6.0.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive-metastore/0.6.0/hive-metastore-0.6.0.jar:/Users/lramos/.m2/repository/org/smithmicro/analytics/hive-libfb303/0.6.0/hive-libfb303-0.6.0.jar:/Users/lramos/.m2/repository/org/jruby/rack/jruby-rack/1.0.8/jruby-rack-1.0.8.jar', '-client', '-Xmx384m', '-Xbootclasspath/a:/Users/lramos/.m2/repository/org/jruby/jruby-complete/1.6.1/jruby-complete-1.6.1.jar', 'org.jruby.Main', '/Users/lramos/.rvm/gems/jruby-1.6.1/bin/rake', 'environment', 'resque:work', 'QUEUE=*'
          #puts "These are the worker's settings:"
          puts java_path, '-cp', class_path, '-client', java_args, boot_path, jruby_main, rake_path, '-f', rake_file, resque_cmd, queue
          @pid = DaemonSpawn.spawnp java_path, '-cp', class_path, '-client', java_args, boot_path, jruby_main, rake_path, '-f', rake_file, resque_cmd, queue
          return @pid
        end
      end
      puts "Worker Not Started. Make sure you are not on Windows, and you have Java and Rake installed. Classpath should contain jruby-complete jar."
      nil
    end

    # The +stop+ method attempts to gracefully shutdown a worker (waits for it to finish).
    # === Parameters
    # * _role_ = role : Role
    def stop
      worker = Resque::Worker.find("#{Socket.gethostname}:#{@pid}:#{@queues}")
      if worker.nil?
        "Could not Find Worker"
      else
        worker.try(:shutdown)
      end
    end

    def restart
      #TODO:Implement
      Resque.workers.each do |worker|
        puts "#{worker.to_s.split(/:/).second}"
      end
    end

  end

  module WorkerManager
    @queue = '*' #Default QUEUE if none

    include WorkerGroups

    def self.create_workers(queue='*', count=1, interval=5, verbose=0)
      unless Helpers.is_windows?
        @workers_pids = Array.new
        #Before Starting the Worker, setup methods in WorkerGroup Module
        WorkerGroups.queue(queue)

        # Clean up after dead group from before -- and become one
        unless WorkerGroups.group_running?
          puts "--- Cleaning up after previous group (PID: #{WorkerGroups.group_master_pid})"
          destroy_workers_and_remove_pids_for_group
          puts "--- Succesfull cleaned up."
        end

        puts "--- Launching #{count} worker(s) on '#{queue}' queue(s) with PID #{Process.pid}"
        count.times do
          #No need to persist the instances, the manager can be used offline
          @workers_pids << Worker.new(queue,interval,verbose).start
        end

        # Pick up the new worker group and create pid files for monitoring
        if WorkerGroups.group?
          # Make sure we have directory for pids
          FileUtils.mkdir_p WorkerGroups.pid_directory.to_s
          FileUtils.mkdir_p WorkerGroups.pid_directory_for_group.to_s
          # Create PID files for workers
          File.open( WorkerGroups.pid_directory.join("#{queue}.pid").to_s, 'w' ) do |f| f.write Process.pid end
          @workers_pids.each do |pid|
            File.open( WorkerGroups.pid_directory_for_group.join("worker_#{pid}.pid").to_s, 'w' ) { |f| f.write pid }
          end
        end
        @workers_pids
      end
    end

    #Kill Worker
    def self.destroy_worker(pid)
      puts "Sending SIGKILL to #{pid}"
      if pid.to_i > 0
        Process.kill("SIGKILL", pid)
        puts "Killed worker with PID #{pid}"
      end
      #Jruby Bug: Process.kill does not return Correct Value
      #I was having problems earlier, use JRuby >=1.6.0
      #http://jira.codehaus.org/browse/JRUBY-4468
      rescue Errno::ESRCH => e
        puts " STALE worker with PID #{pid}"
    end

    #Kill Workers
    def self.destroy_workers
      Resque::Worker.all.each do |worker|
        puts "Killing worker #{worker}"
        host, pid, queues = worker.id.split(':')
        if pid.to_i > 0
          res = Process.kill("SIGKILL", pid.to_i)
        end
      end
      rescue Errno::ESRCH => e
          puts " Did not find worker: #{e.message}"
    end

    #Kill Workers and delete PID Files
    def self.destroy_workers_and_remove_pids_for_group
      Dir.glob(WorkerGroups.pid_directory_for_group.join('worker_*.pid').to_s) do |pidfile|
        begin
          puts "...beginning to destoy."
          pid = pidfile[/(\d+)\.pid/, 1].to_i
          destroy_worker(pid)
        ensure
          FileUtils.rm pidfile, :force => true
        end
      end
      puts "Cleaning up master pid group."
      if WorkerGroups.group_master_pid
        FileUtils.rm    WorkerGroups.pid_directory.join("#{@queue}.pid").to_s
        FileUtils.rm_rf WorkerGroups.pid_directory_for_group.to_s
      end
    end

    #Attempt Safe Clean Shutdown
    def self.safe_shutdown_workers
      Resque::Worker.all.each do |worker|
        puts "Attempting to Shut down worker #{worker}"
        worker.try(:shutdown)
        worker.shutdown
      end
    end


    #Force Shutdown
    def self.shutdown_workers(queue='*')
      #Before Shutting Down, setup WorkerGroup Queue
      WorkerGroups.queue(queue)
      #Check if there is a group created for the current Queue
      if WorkerGroups.group?
        destroy_workers_and_remove_pids_for_group
      else
        destroy_workers
      end
      true
    end

    def self.restart_all
      #TODO: Implement
    end
  end
end
