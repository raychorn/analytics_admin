class DataParametersController < ApplicationController
  # GET /data_parameters
  # GET /data_parameters.xml
  def index
    @data_parameters = DataParameter.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @data_parameters.to_xml(:include => :value_mappings) }
    end
  end

  # GET /data_parameters/1
  # GET /data_parameters/1.xml
  def show
    @data_parameter = DataParameter.find(params[:id])

    respond_to do |format|
      format.html # show.html.erb
      format.xml  { render :xml => @data_parameter }
    end
  end

  # GET /data_parameters/new
  # GET /data_parameters/new.xml
  def new
    @data_parameter = DataParameter.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @data_parameter }
    end
  end

  # GET /data_parameters/1/edit
  def edit
    @data_parameter = DataParameter.find(params[:id])
  end

  # POST /data_parameters
  # POST /data_parameters.xml
  def create
    @data_parameter = DataParameter.new(params[:data_parameter])

    respond_to do |format|
      if @data_parameter.save
        format.html { redirect_to(@data_parameter, :notice => 'Data parameter was successfully created.') }
        format.xml  { render :xml => @data_parameter, :status => :created, :location => @data_parameter }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @data_parameter.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /data_parameters/1
  # PUT /data_parameters/1.xml
  def update
    @data_parameter = DataParameter.find(params[:id])

    respond_to do |format|
      if @data_parameter.update_attributes(params[:data_parameter])
        format.html { redirect_to(@data_parameter, :notice => 'Data parameter was successfully updated.') }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @data_parameter.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /data_parameters/1
  # DELETE /data_parameters/1.xml
  def destroy
    @data_parameter = DataParameter.find(params[:id])
    @data_parameter.destroy

    respond_to do |format|
      format.html { redirect_to(data_parameters_url) }
      format.xml  { head :ok }
    end
  end
end
