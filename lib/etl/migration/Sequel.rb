require 'sequel'
require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Sequel
  	def initialize(table, version)
  	  @table = table.to_sym
  	  @version = version 
  	  config_dir = ETL.config.config_dir
      table_map_config_file = config_dir + "/sequel_table_maps.yml"
      raise "Could not find config file under #{config_dir}" unless File.file?(table_map_config_file)
      config_values = ETL::HashUtil.symbolize_keys(Psych.load_file(table_map_config_file)) 

      raise "#{@table} is not defined in config file" unless config_values.include? @table
      table_map = config_values[@table]

      @source_db_params = table_map[:source_db_params] || { host: "localhost", adapter: "mysql2", database: "mysql", user: "root", password: "" }
      @columns = table_map[:columns] || {} 
      @migration_dir = table_map[:migration_file_dir] || "../etc" 
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
      	  h[@columns[column_name].to_sym] = data_type
      	end 
      end
    end

    def destination_columns
      @destination_columns ||= begin
	    sql =<<SQL
	    	select "column" from pg_table_def where tablename = '#{@table}';
SQL
	    result_columns = @redshiftclient.execute(sql)
	    result_columns.map { |result| result["column"].to_sym }
      end
    end

    def need_migration?
      return true if @version == 0
      schema_map.keys.sort == destination_columns.sort
    end

    # create a migration with a given version
    def generate_migration
      if @version > 0
      	sql =<<SQL
      	  alter table #{@table} rename to #{table}_#{@version}
SQL
        @redshiftclient.execute(sql)
        up = get_up
        down = get_down
       else
        up = get_up
        down = "" 
       end

s =<<END
Sequel.migration do
  up do
  	#{up}
  end

  down do
  	#{down}
  end
end
END
      File.open("#{@migration_dir}/#{(@version+1).to_s.rjust(4, '0')}_#{@table}.rb", "w") do |f|
        f << s
      end
    end

    # create table for migration
    def get_up
      column_array = schema_map.map { |column, type| "#{column} #{type}" }
      up =<<END
  create table #{@table} ( #{column_array.join(", ")} )
END
    end

    # drop table for migration
    def get_down 
      down = <<END
  drop_table(#{@table})
END
    end
  end
end