class RolesController < ApplicationController
  def show
    @role = Role.find(params[:id])

    respond_to do |format|
      format.json  { render :json => @role }
    end
  end

  def new
    @role =  Role.new

    respond_to do |format|
      format.json  { render :json => @role }
    end
  end

  def edit
    @role =  Role.find(params[:id])

    respond_to do |format|
      format.json  { render :json => @role }
    end
  end
end
