require 'java'
class HomeController < ApplicationController
  def index
    java_import java.lang.System
    version = System.getProperties["java.runtime.version"]
    java.lang.System.out.println(version)
    render :json => {"java_version" => version}.to_json
  end
end
