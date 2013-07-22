require 'analytics/analytics_app'

module Hadoop
  class HadoopApp < Sinatra::Base
    set :root, File.dirname(__FILE__)
    helpers Analytics::Helpers

    def initialize(*args)
        super
        @debug = ENV['DEBUG']
    end

    get '/hadoop' do
      "Hadoop Root"
    end

  end
end