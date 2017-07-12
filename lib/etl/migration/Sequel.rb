require 'mysql2'
require 'sequel'
require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Sequel
  	def initialize(table)
  	  @table = table.to_sym
  	  config_dir = ETL.config.config_dir
      table_map_config_file = config_dir + "/sequel_table_maps.yml"
      raise "Could not find config file under #{config_dir}" unless File.file?(table_map_config_file)
      config_values = ETL::HashUtil.symbolize_keys(Psych.load_file(table_map_config_file)) 

      raise "#{@table} is not defined in config file" unless config_values.include? @table
      table_map = config_values[@table]

      @source_db_params = table_map[:source_db_params] || { host: "localhost", adapter: "mysql2", database: "mysql", user: "root", password: "" }
      @columns = table_map[:columns] || {} 
      @redshift_params = table_map[:destination_db_params] || { host: "localhost", port: 5439, database: "dev", user: "masteruser", password: "" }
      @redshiftclient = ::ETL::Redshift::Client.new(@redshift_params)
  	end

  	def source_db_connect
  	  @source_db_connect ||= ::Sequel.connect(@source_db_params)
  	end

    def schema_map
      @schema_map ||= begin
      	all_schema = source_db_connect.fetch("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '#{@table}' ").all
      	all_schema.each_with_object({}) do |column, h|
      	  column_name = column[:COLUMN_NAME].to_sym
      	  next unless @columns.keys.include? column_name 
      	  data_type = column[:DATA_TYPE]
      	  data_type += "(#{column[:CHARACTER_MAXIMUM_LENGTH]})" if column[:CHARACTER_MAXIMUM_LENGTH]
      	  h[@columns[column_name]] = data_type
      	end 
      end
    end

    # create table for migration
    def up
      column_array = schema_map.map { |column, type| "#{column} #{type}" }
      sql = <<SQL
        create table #{@table} ( #{column_array.join(", ")} )
SQL
      @redshiftclient.execute(sql)
    end

    # drop table for migration
    def down 
      @redshiftclient.drop_table
    end
  end
end