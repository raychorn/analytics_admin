module HiveJobSpecHelpers
  def self.create_run_options(num=1,store_in_db=false)
    options = {}
    options["database_name"] = "default"
    options["query_results"] = create_run_options_query_results(num,store_in_db)
    options
  end

  #HiveJobSpecHelpers.create_run_options_query_results(1,true,"#PARENT").each { |q| options["query_results"] << q }
  def self.create_run_options_query_results(num=1,store_in_db=false,table_name='testHiveDriverTable')
    query_results = []
    rnd = rand(50)
    num.to_i.times { |i| query_results << Hash["query_result",
                                          Hash["query_string","select key,value from #{table_name}",
                                               "store_in_db",store_in_db,
                                               "id",i+rnd]]
    }
    query_results
  end
end