require 'rbconfig'
require 'analytics/helpers/pretty_printing'
require 'analytics/helpers/sinatra'

module Analytics
  module Helpers
    include Analytics::Helpers::PrettyPrinting
    include Analytics::Helpers::Sinatra

    #
    # helpers defined here are available to all views and sinatra routes
    #
    
    # debug { puts "hi!" }
    def debug
      yield if @debug
    end

    # sha(hash) => '01578ad840f1a7eba2bd202351119e635fde8e2a'
    def sha(thing)
      Digest::SHA1.hexdigest(thing.to_s)
    end
    
    # for sorting hashes with symbol keys
    def sort_hash(hash)
      hash.to_a.sort_by { |a, b| a.to_s }
    end

    #Returns true if windows
    def self.is_windows?
      (RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/)
    end

    #Returns Jar full path if found in the classpath
    def self.which_jar(jar)
      search_path = java.lang.System.getProperties['java.class.path'].to_s +
                    java.lang.System.getProperties['java.boot.class.path'].to_s +
                    java.lang.System.getProperties['sun.boot.class.path'].to_s +
                    Dir[Rails.root.to_s+"/lib/*.jar"].join(':')
      search_path.split(File::PATH_SEPARATOR).each do |path|
        jar_path = "#{path}"
        return jar_path if jar_path.include?(jar)
      end
      nil
    end

    #Returns full path of command if found in the system's path
    def self.which(cmd)
      exts = ENV['PATHEXT'] ? ENV['PATHEXT'].split(';') : ['']
      paths = ENV['PATH'].split(File::PATH_SEPARATOR)
      paths.push(Dir[Rails.root.to_s+"/gems/bin"])
      paths.each do |path|
        exts.each { |ext|
          exe = "#{path}/#{cmd}#{ext}"
          return exe if File.executable? exe
        }
      end
      nil
    end

    #Returns boolean value from string, true if match, false otherwise
    def self.to_bool(field)
      field.match(/(true|t|yes|y|1)$/i) != nil
    end
  end
end

class String
  #Returns boolean value from string, true if match, false otherwise
  def to_bool
    self.match(/(true|t|yes|y|1)$/i) != nil
  end
end