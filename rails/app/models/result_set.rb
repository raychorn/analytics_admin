class ResultSet
  # required for xml serialization
  extend ActiveModel::Naming
  include ActiveModel::Serializers::Xml

  attr_accessor :result_type, :dynamic_table_klass, :final_sql_string, :latest_data_results

  def initialize(options = {})
    options ||= {}

    @result_type = options[:result_type]
    @dynamic_table_klass = options[:dynamic_table_klass]
    @final_sql_string = options[:final_sql_string]
    @latest_data_results = options[:latest_data_results]
  end

  def attributes
    {
        'result_type' => self.result_type,
        'column_names' => self.column_names,
        'results' => self.results
    }
  end

  def column_names
    if self.result_type == 'active_record'
      klass = self.dynamic_table_klass

      if klass.table_exists?
        column_names = klass.columns.collect { |c| c.name }
        column_names.delete('id')
        column_names
      else
        []
      end
    else # json
      latest_data_results.blank? ? [] : latest_data_results[0]['columns']
    end
  end

  def results
    if self.result_type == 'active_record'
      if dynamic_table_klass.table_exists?
        if self.final_sql_string.present?
          raw_res = dynamic_table_klass.find_by_sql(self.final_sql_string)
        else
          raw_res = dynamic_table_klass.all
        end
        raw_res.collect { |raw| raw.attributes }
      else
        []
      end
    else # json
      # return on the latest one for now
      latest_data_results.blank? ? [] : latest_data_results[0]
    end
  end
end
