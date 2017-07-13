require_relative '../command'
require 'etl/job/exec'
require 'sequel'
require 'erb'

module ETL::Cli::Cmd
  class Migration < ETL::Cli::Command

    class Create < ETL::Cli::Command
      parameter "table_name", "Table name we create migration against", required: true

      option ['-p', '--provider'], "Provider", attribute_name: :provider
      option ['-h', '--host'], "Host", attribute_name: :host
      option ['-u', '--user'], "User", attribute_name: :user
      option ['-pw', '--password'], "Password", attribute_name: :password
      option ['-db', '--database'], "Database", attribute_name: :database
      option ['--inputdir'], "Input directory that contains a configuration file", attribute_name: :input_dir, default: "../../etc"
      option ['--outputdir'], "Output directory where migration is created at", attribute_name: :output_dir, default: "db/migrations"

      Adopter = { mysql: mysql2 }

      class Generator
        attr_accessor :up, :down
        def template_binding
          binding
        end
      end

      def config_value
        @config_values ||= begin
          config_file = input_dir + "/migration_config.yml"
          raise "Could not find migration_config.yml file under #{input_dir}" unless File.file?(config_file)
          ETL::HashUtil.symbolize_keys(Psych.load_file(config_file))
          raise "#{table_name} is not defined in the config file" unless config_values.include? table_name
          config_values[table_name]
        end
      end

      def provider_params
        @provider_params ||= begin
          if provider && host && user && password && database
            adapter = provider
            adapter = Adopter[provider] if Adopter.include? provider
            return { host: host, adapter: adapter, database: database, user: user, password: password } 
          else
            raise "source_db_params is not defined in the config file" unless config_values.include? :source_db_params
            return config_values[:source_db_params]
          end  
          raise "Parameters to connect to the data source are required"
        end
      end

      def columns
        @columns ||= begin
          raise "columns is not defined in the config file" unless config_values.include? :columns 
          config_values[:columns]
        end
      end

      def provider_connect
        @provider_connect ||= ::Sequel.connect(provider_params)
      end

      def schema_map
        @schema_map ||= begin
          all_schema = source_db_connect.fetch("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '#{@table}' ").all
          all_schema.each_with_object({}) do |column, h|
            column_name = columns[column[:COLUMN_NAME].to_sym]
            h[column_name] = [ column[:DATA_TYPE].to_sym, column[:CHARACTER_MAXIMUM_LENGTH] ]
          end
        end
      end

      def four_digit_str(i)
        i.to_s.rjust(4, "0")
      end

      def migration_version
        @migration_version ||= Dir["#{output_dir}/*#{table_name}.rb"].length
      end

      def create_migration(up, down="")
        generator = Generator.new
        migration_file = File.open("#{output_dir}/#{four_digit_str(migration_version+1)}_#{table_name}.rb")
        template = File.read("../../../../etc/erb_templates/redshift_migration.erb")
        generator.up = up
        generator.down = down 
        migration_file << ERB.new(template).result(generator.template_binding)
        migration_file.close
      end

      def execute
        column_array = schema_map.map do |column, types|
          type = "#{types[0]}"
          type += "(#{types[1]})" if types[1]
          "#{column} #{type}" 
        end

        up = " run 'create table #{@table} ( #{column_array.join(", ")} )' "
        create_migration(up)
      end
    end

    subcommand 'create', 'Create migration', Migration::Create
  end
end
