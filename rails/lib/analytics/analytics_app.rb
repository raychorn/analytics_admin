require 'analytics/helpers'
require 'analytics/worker'
module Analytics
  class AnalyticsApp < Sinatra::Base
    set :root, File.dirname(__FILE__)
    helpers Analytics::Helpers

    def initialize(*args)
        super
        @debug = ENV['DEBUG']
    end
    
    #
    # routes
    #
    get '/analytics' do
      html = "<h3>Analytics</h3><p><%= @java_classpath %></p><p><%= @java_bootclass %></p><p><%=@tomcat_classpath %></p>"
      @java_classpath = "Java Classpath - #{java.lang.System.getProperties['java.class.path']}"
      @java_bootclass = "Java xBootclasspath - #{java.lang.System.getProperties['sun.boot.class.path']}"
      @tomcat_classpath = "Tomcat Classpath - #{Dir[Rails.root.to_s+"/lib/*.jar"].join(':')}"
      #"Analytics: Java Classpath - #{java.lang.System.getProperties["java.class.path"]}"
      erb html
    end

    get '/analytics/start-workers' do

      if Helpers.is_windows?
      "Windows Platform not Supported. Please start a Worker Process using Rake."
      elsif !Helpers.which('java') or !Helpers.which('rake')
      "Java or Rake are not in the system's path. Contact administrator."
      else
      queue = params[:queue] ? params[:queue] : '*'
      interval = params[:interval] ? params[:interval] : '5'
      count = params[:count] ? params[:count] : '1'
      verbose = params[:vebose] ? params[:verbose] : '0'
      pids = Analytics::WorkerManager.create_workers(queue,count.to_i,interval.to_i,verbose.to_i)
      if pids.include?(nil)
        "Worker not started, check logs..."
      else
        "Starting #{count} worker(s) on '#{queue}': [#{pids.to_s}], check logs..."
      end
      end

    end

    get '/analytics/stop-workers' do
      queue = params[:queue] ? params[:queue] : '*'
      res = Analytics::WorkerManager.shutdown_workers(queue)
      "Shuting down workers: #{res.to_s}"
    end

    get '/analytics/restart-workers' do
      #TODO: Implement
    end
    
  end
end