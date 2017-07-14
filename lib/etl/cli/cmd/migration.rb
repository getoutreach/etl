require_relative '../command'
require 'etl/job/exec'
require 'sequel'
require 'erb'

module ETL::Cli::Cmd
  class Migration < ETL::Cli::Command

    class Create < ETL::Cli::Command

      option "--table", "TABLE", "table name", :required => true
      option ['-p', '--provider'], "Provider", attribute_name: :provider
      option ['-h', '--host'], "Host", attribute_name: :host
      option ['-u', '--user'], "User", attribute_name: :user
      option ['-pw', '--password'], "Password", attribute_name: :password
      option ['-db', '--database'], "Database", attribute_name: :database
      option "--inputdir", "Input directory that contains a configuration file", :attribute_name => :inputdir, :required => true
      option "--outputdir", "Output directory where migration is created at", :attribute_name => :outputdir, :required => true 

      Adopter = { mysql: "mysql2" }

      class Generator
        attr_accessor :up, :down
        def template_binding
          binding
        end
      end

      def table_config
        @table_config ||= begin
          config_file = @inputdir + "/migration_config.yml"
          raise "Could not find migration_config.yml file under #{@inputdir}" unless File.file?(config_file)
          config_values = ETL::HashUtil.symbolize_keys(Psych.load_file(config_file))
          puts "config_values #{config_values}"
          raise "#{table} is not defined in the config file" unless config_values.include? table.to_sym
          config_values[table.to_sym]
        end
      end

      def provider_params
        @provider_params ||= begin
          if @provider && @host && @user && @password && @database
            adapter = @provider
            adapter = Adopter[@provider] if Adopter.include? @provider
            return { host: host, adapter: adapter, database: database, user: user, password: password } 
          else
            raise "source_db_params is not defined in the config file" unless table_config.include? :source_db_params
            return table_config[:source_db_params]
          end  
          raise "Parameters to connect to the data source are required"
        end
      end

      def columns
        @columns ||= begin
          raise "columns is not defined in the config file" unless table_config.include? :columns 
          table_config[:columns]
        end
      end

      def provider_connect
        @provider_connect ||= ::Sequel.connect(provider_params)
      end

      def source_schema
        @source_schema ||= provider_connect.fetch("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '#{@table}' ").all
      end

      def schema_map
        @schema_map ||= begin
          source_schema.each_with_object({}) do |column, h|
            column_name = columns[column[:COLUMN_NAME].to_sym]
            h[column_name] = [ column[:DATA_TYPE].to_sym, column[:CHARACTER_MAXIMUM_LENGTH] ]
          end
        end
      end

      def four_digit_str(i)
        i.to_s.rjust(4, "0")
      end

      def migration_version
        @migration_version ||= Dir["#{@outputdir}/*_#{table}.rb"].length
      end

      def create_migration(up, down="")
        generator = Generator.new
        migration_file = File.open("#{@outputdir}/#{four_digit_str(migration_version+1)}_#{table}.rb", "w")
        template = File.read("#{@inputdir}/redshift_migration.erb")
        generator.up = up
        generator.down = down 
        migration_file << ERB.new(template).result(generator.template_binding)
        migration_file.close
      end

      def up_sql
        column_array = schema_map.map do |column, types|
          type = "#{types[0]}"
          type += "(#{types[1]})" if types[1]
          "#{column} #{type}" 
        end

        "create table #{@table} ( #{column_array.join(", ")} )"
      end

      def execute
        create_migration(up_sql)
      end
    end

    subcommand 'create', 'Create migration', Migration::Create
  end
end
