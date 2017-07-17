module ETL::Job

  # Base class for all migratable jobs that are run
  class MigratableBase < Base

    def migration_config
      @migration_config ||= begin
        config_dir = ETL.config.config_dir
        org_table_config_file = config_dir + "/migration_config.yml"
        migration_config = {}
        migration_config = ETL::HashUtil.symbolize_keys(Psych.load_file(org_table_config_file)) if File.file?(org_table_config_file)
      end
    end

    def migration_files
      migration_dir = migration_config.fetch(:migration_dir, "db/migrations")
      Dir["#{migration_dir}/*_#{id}.rb"]
    end

    def migrate
      migration_version = migration_config.fetch(:migration_version, 0)
      # executt migration
      migration_files.each do |file|
        next unless file.split('/').last[0..3].to_i > migration_version
        load file
        Migration.execute
      end
    end

    def run
      migrate
      super
    end
  end
end
