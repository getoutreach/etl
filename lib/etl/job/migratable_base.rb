require 'migration/sequel'

module ETL::Job

  # Base class for all migratable jobs that are run
  class MigratableBase < Base

    def migration
      @migration ||= ETL::Migration::Sequel.new(id)
    end

    def migrate
      return unless migration.need_migration?
      migration.generate_migration
      # execute migratior
      migration_version = migration_version.migration_version
      while migration_version < migration_version.current_version
        system("sequel -m #{migration.migration_dir}/#{migration.four_digit_str(migration_version)}_#{id}.rb redshift://#{migration.redshift_params[:host]}/#{migration.redshift_params[:database]}")
        migration_version += 1
      end
      migration.bump_version(migration_version)
    end

    def input
      sql = <<SQL
        SELECT #{migration.schema_map.keys.join(", ")} FROM #{@table}
SQL
      ETL::Input::Sequel.new(migration.source_db_params, sql)
    end

    def output
      output_class.new(id, migration.schema_map)
    end

    def output_class
      Class.new(ETL::Output::Redshift) do
        def feed_name
          @dest_table
        end

        def aws_params
          ETL.config.aws[:etl]
        end

        def redshift_params
          ETL.config.redshift[:etl]
        end

        def initialize(table, schema_map)
          super(:upsert, redshift_params, aws_params)

          @dest_table = table 

          define_schema do |s|
            schema_map.each do |key, types|
              case types[0]
              when :int
                s.int(key.to_sym)
              when :float
                s.float(key.to_sym)
              when :double
                s.double(key.to_sym)
              when :string
                s.string(key.to_sym)
              when :character
                s.character(key.to_sym)
              when :date
                s.date(key.to_sym)
              when :varchar
                s.varchar(key.to_sym, types[1])
              end
            end
          end
        end
      end
    end
  end
end
