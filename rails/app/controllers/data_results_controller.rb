class DataResultsController < ApplicationController
  # GET /data_results
  # GET /data_results.xml
  def index
    @data_results = DataResult.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @data_results }
    end
  end

  # GET /data_results/1
  # GET /data_results/1.xml
  def show
    @data_result = DataResult.find(params[:id])

    respond_to do |format|
      format.html # show.html.erb
      format.xml  { render :xml => @data_result }
    end
  end

  # GET /data_results/new
  # GET /data_results/new.xml
  def new
    @data_result = DataResult.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @data_result }
    end
  end

  # GET /data_results/1/edit
  def edit
    @data_result = DataResult.find(params[:id])
  end

  # POST /data_results
  # POST /data_results.xml
  def create
    @data_result = DataResult.new(params[:data_result])

    respond_to do |format|
      if @data_result.save
        format.html { redirect_to(@data_result, :notice => 'DataResult was successfully created.') }
        format.xml  { render :xml => @data_result, :status => :created, :location => @data_result }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @data_result.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /data_results/1
  # PUT /data_results/1.xml
  def update
    @data_result = DataResult.find(params[:id])

    respond_to do |format|
      if @data_result.update_attributes(params[:data_result])
        format.html { redirect_to(@data_result, :notice => 'DataResult was successfully updated.') }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @data_result.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /data_results/1
  # DELETE /data_results/1.xml
  def destroy
    @data_result = DataResult.find(params[:id])
    @data_result.destroy

    respond_to do |format|
      format.html { redirect_to(data_results_url) }
      format.xml  { head :ok }
    end
  end
end
