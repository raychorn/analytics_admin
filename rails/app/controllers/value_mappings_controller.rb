class ValueMappingsController < ApplicationController
  # GET /value_mappings
  # GET /value_mappings.xml
  def index
    @value_mappings = ValueMapping.all

    respond_to do |format|
      format.html # index.html.erb
      format.xml  { render :xml => @value_mappings }
    end
  end

  # GET /value_mappings/1
  # GET /value_mappings/1.xml
  def show
    @value_mapping = ValueMapping.find(params[:id])

    respond_to do |format|
      format.html # show.html.erb
      format.xml  { render :xml => @value_mapping }
    end
  end

  # GET /value_mappings/new
  # GET /value_mappings/new.xml
  def new
    @value_mapping = ValueMapping.new

    respond_to do |format|
      format.html # new.html.erb
      format.xml  { render :xml => @value_mapping }
    end
  end

  # GET /value_mappings/1/edit
  def edit
    @value_mapping = ValueMapping.find(params[:id])
  end

  # POST /value_mappings
  # POST /value_mappings.xml
  def create
    @value_mapping = ValueMapping.new(params[:value_mapping])

    respond_to do |format|
      if @value_mapping.save
        format.html { redirect_to(@value_mapping, :notice => 'Value mapping was successfully created.') }
        format.xml  { render :xml => @value_mapping, :status => :created, :location => @value_mapping }
      else
        format.html { render :action => "new" }
        format.xml  { render :xml => @value_mapping.errors, :status => :unprocessable_entity }
      end
    end
  end

  # PUT /value_mappings/1
  # PUT /value_mappings/1.xml
  def update
    @value_mapping = ValueMapping.find(params[:id])

    respond_to do |format|
      if @value_mapping.update_attributes(params[:value_mapping])
        format.html { redirect_to(@value_mapping, :notice => 'Value mapping was successfully updated.') }
        format.xml  { head :ok }
      else
        format.html { render :action => "edit" }
        format.xml  { render :xml => @value_mapping.errors, :status => :unprocessable_entity }
      end
    end
  end

  # DELETE /value_mappings/1
  # DELETE /value_mappings/1.xml
  def destroy
    @value_mapping = ValueMapping.find(params[:id])
    @value_mapping.destroy

    respond_to do |format|
      format.html { redirect_to(value_mappings_url) }
      format.xml  { head :ok }
    end
  end
end
