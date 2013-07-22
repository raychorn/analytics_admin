require 'java'

require 'analytics/analytics_app'
require 'hive/hive_server'

java_package 'org.smithmicro.analytics.hive'

module Hive
  include HiveServer
  class HiveApp < Sinatra::Base
    set :root, File.dirname(__FILE__)
    helpers Analytics::Helpers

    def initialize(*args)
        super
        @debug = ENV['DEBUG']
    end

    get '/hive' do
        is_online = false
        HiveServer.connect(HiveServer.rails_env["remote_hive_server"].to_s) do | connection |
          is_online = connection.online?
        end
        "Hive Service Online: #{is_online}"
    end

  end
end