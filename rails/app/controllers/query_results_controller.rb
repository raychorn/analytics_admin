class QueryResultsController < ApplicationController
  # GET /query_results
  # GET /query_results.xml
  def index
    @query_results = QueryResult.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @query_results }
    end
  end

  # GET /query_results/1
  # GET /query_results/1.xml
  def show
    sample_option = params[:sample_option]
    if sample_option.present?
      if QueryResult::Sample.real_data?(sample_option.to_i)
        @query_result = QueryResult.find(params[:id])
      else
        @query_result = QueryResult.new
        @query_result.id = params[:id]
      end
      @query_result.sample_option = params[:sample_option].to_i
    else # default
      @query_result = QueryResult.find(params[:id])
    end

    @query_result.sql_string = params[:sql_string]

    respond_to do |format|
      format.html # show.html.erb
      format.xml { render :xml => @query_result.to_xml(:methods => :result_set) }
    end
  end

  # GET /query_results/new
  # GET /query_results/new.xml
  def new
    @query_result = QueryResult.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @query_result }
    end
  end

  # GET /query_results/1/edit
  def edit
    @query_result = QueryResult.find(params[:id])
  end

  # POST /query_results
  # POST /query_results.xml
  def create
    @query_result = QueryResult.new(params[:query_result])
    @query_result.setup_associations

    respond_to do |format|
      if @query_result.save
        format.html { redirect_to(@query_result, :notice => 'Query result was successfully created.') }
        format.xml  { render :xml => @query_result, :status => :created, :location => @query_result }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @query_result.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /query_results/1
  # PUT /query_results/1.xml
  def update
    @query_result = QueryResult.find(params[:id])

    respond_to do |format|
      if @query_result.update_attributes(params[:query_result])
        # TODO move elsewhere
        @query_result.setup_associations
        @query_result.save!
        format.html { redirect_to(@query_result, :notice => 'Query result was successfully updated.') }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @query_result.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /query_results/1
  # DELETE /query_results/1.xml
  def destroy
    @query_result = QueryResult.find(params[:id])
    @query_result.destroy

    respond_to do |format|
      format.html { redirect_to(query_results_url) }
      format.xml  { head :ok }
    end
  end
end
