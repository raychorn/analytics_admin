require_dependency "#{Rails.root}/lib/analytics/worker.rb"
require 'java'

namespace :analytics do
  namespace :workers do
      desc <<-DESC
        Start or Stop Analytics Workers.
        Run using the commands:
          'rake analytics:workers:start'
          'rake analytics:workers:stop'
      DESC

      task :start => [:environment] do

        ENV['QUEUE']   ||= '*'
        ENV['COUNT']   ||= '1'
        ENV['INTERVAL'] ||= '5'
        ENV['VERBOSE'] ||= '0'

        pids = Analytics::WorkerManager.create_workers(ENV['QUEUE'],ENV['COUNT'].to_i,ENV['INTERVAL'].to_i,ENV['VERBOSE'].to_i)
        "Started #{ENV['COUNT']} worker(s) on '#{ENV['QUEUE']}': [#{pids.to_s}]"

      end

      task :stop => [:environment] do

        ENV['QUEUE']   ||= '*'

        res = Analytics::WorkerManager.shutdown_workers(ENV['QUEUE'])
        "Shuting down workers: #{res.to_s}"

      end
  end
end
