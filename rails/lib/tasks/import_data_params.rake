require_dependency "#{Rails.root}/db/seed_data_helper.rb"

namespace :app do
  desc <<-DESC
    Load data params from csv.
    Run using the command 'rake app:import_data_params'
  DESC
  task :import_data_params => [:environment] do

    DataParameter.delete_all
    ValueMapping.delete_all

    default_group = Group.create_or_update_by(:name, 'default')

    #csv_path = "#{Rails.root}/db/seed_data/data_parameters.csv"
    #SeedDataHelper.load_data_params_from_csv(default_group, csv_path)

    csv_path = "#{Rails.root}/db/seed_data/data_parameters_custom.csv"
    SeedDataHelper.load_data_params_from_csv(default_group, csv_path)

  end
end
