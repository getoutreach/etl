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

    def migrate
      migration_dir = migration_config.fetch(id, "db/migrations")
      # execute migration
      migration_version = migration_config.fetch(:migration_version, 0)
      migration_files = Dir["#{migration_dir}/*_#{id}.rb"]
      migration_files.each do |file|
        next unless file.split('/').last[0..3].to_i > migration_version
        require file
        m = Migration::Redshift.new
        m.up
        m.down
      end
    end
  end
end
