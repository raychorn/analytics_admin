class JobRunnersController < ApplicationController
  # GET /job_runners
  # GET /job_runners.xml
  def index
    @job_runners = JobRunner.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @job_runners }
    end
  end

  # GET /job_runners/1
  # GET /job_runners/1.xml
  def show
    @job_runner = JobRunner.find(params[:id])

    respond_to do |format|
      format.html # show.html.erb
      format.xml  { render :xml => @job_runner }
    end
  end

  # GET /job_runners/new
  # GET /job_runners/new.xml
  def new
    @job_runner = JobRunner.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @job_runner }
    end
  end

  # GET /job_runners/1/edit
  def edit
    @job_runner = JobRunner.find(params[:id])
  end

  # POST /job_runners
  # POST /job_runners.xml
  def create
    @job_runner = JobRunner.new(params[:job_runner])
    @job_runner.setup_associations

    respond_to do |format|
      if @job_runner.save
        format.html { redirect_to(@job_runner, :notice => 'Job runner was successfully created.') }
        format.xml  { render :xml => @job_runner, :status => :created, :location => @job_runner }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @job_runner.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /job_runners/1
  # PUT /job_runners/1.xml
  def update
    @job_runner = JobRunner.find(params[:id])
    if params[:job_runner][:query_results].nil?
      params[:job_runner].delete(:query_results)
    end
    respond_to do |format|
      if @job_runner.update_attributes(params[:job_runner])
        # TODO move setup_associations and save! elsewhere
        @job_runner.setup_associations
        @job_runner.save!
        format.html { redirect_to(@job_runner, :notice => 'Job runner was successfully updated.') }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @job_runner.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /job_runners/1
  # DELETE /job_runners/1.xml
  def destroy
    @job_runner = JobRunner.find(params[:id])
    @job_runner.destroy

    respond_to do |format|
      format.html { redirect_to(job_runners_url) }
      format.xml  { head :ok }
    end
  end

  def start
    @job_runner = JobRunner.find(params[:id])
    @job_runner.start

    respond_to do |format|
      format.html { redirect_to(job_runners_url) }
      format.xml  { render :xml => @job_runner }
    end
  end

  def enable_schedule
    @job_runner = JobRunner.find(params[:id])
    @job_runner.enable_schedule

    respond_to do |format|
      format.html { redirect_to(job_runners_url) }
      format.xml  { render :xml => @job_runner }
    end
  end

  def disable_schedule
    @job_runner = JobRunner.find(params[:id])
    @job_runner.disable_schedule

    respond_to do |format|
      format.html { redirect_to(job_runners_url) }
      format.xml  { render :xml => @job_runner }
    end
  end
end
