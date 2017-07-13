require 'sequel'
require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Sequel
  	attr_reader :migration_dir, :migration_version, :source_db_params, :redshift_params

    def initialize(table)
      @table = table.to_sym
      config_dir = ETL.config.config_dir
      @table_map_config_file = config_dir + "/sequel_table_maps.yml"
      raise "Could not find config file under #{config_dir}" unless File.file?(@table_map_config_file)
      @config_values = ETL::HashUtil.symbolize_keys(Psych.load_file(@table_map_config_file)) 

      raise "#{@table} is not defined in config file" unless @config_values.include? @table
      table_map = @config_values[@table]

      @source_db_params = table_map[:source_db_params] || { host: "localhost", adapter: "mysql2", database: "mysql", user: "root", password: "" }
      # Use if we want to change column names in the destination table 
      @columns = table_map[:columns] || {} 
      @migration_version = table_map[:migration_version] || 0 
      @migration_dir = table_map[:migration_file_dir] || "../etc" 
      @redshift_params = table_map[:destination_db_params] || { host: "localhost", port: 5439, database: "dev", user: "masteruser", password: "" }
      @redshiftclient = ::ETL::Redshift::Client.new(@redshift_params)
    end

    def current_version
      @current_version ||= Dir["#{migration_dir}/*.rb"].length
    end

    def source_db_connect
      @source_db_connect ||= ::Sequel.connect(@source_db_params)
    end

    def schema_map
      @schema_map ||= begin
      	all_schema = source_db_connect.fetch("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '#{@table}' ").all
      	all_schema.each_with_object({}) do |column, h|
          column_name = column[:COLUMN_NAME].to_sym
          h[column_name] = [ column[:DATA_TYPE].to_sym, column[:CHARACTER_MAXIMUM_LENGTH] ]
        end
      end
    end

    # Use when we want to detect schema changes
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
      return true if @migration_version == 0 || @migration_version > current_version
      #schema_map.keys.sort == destination_columns.sort
    end

    # create a migration with a given migration_version
    def generate_migration
      down = ""
      rename = ""
      rename = "rename_table(#{@table}, #{table}_#{current_version}" if @migration_version > 0
      up = get_up

s =<<END
Sequel.migration do
  up do
  	#{rename}
  end

  down do
  	#{down}
  end

  up {run '#{up}'} 
end
END
      File.open("#{@migration_dir}/#{four_digit_str(@current_version+1)}_#{@table}.rb", "w") do |f|
        f << s
      end
    end

    def four_digit_str(i)
      i.to_s.rjust(4, "0")
    end

    # create table for migration
    def get_up
      column_array = schema_map.map do |column, types|
        type = "#{types[0]}"
        type += "(#{types[1]})" if types[1]
        "#{column} #{type}" 
      end

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

    # Update migration version in config
    def bump_version(version)
      @config_values[@table][:migration_version] = version
      File.open("@table_map_config_file",'w') do |h| 
        h.write @config_values.to_yaml
      end
    end
  end
end