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

    # return an array of files sorted by version
    def migration_files
      migration_dir = migration_config.fetch(:migration_dir, "db/migrations")
      Dir["#{migration_dir}/*_#{id}.rb"]
    end

    def target_version
      @target_version ||= begin
        env_name = "ENV_#{id.upcase}_VERSION"
        version = ENV["#{env_name}"]
        raise "#{env_name} is not set" unless version
        version
      end
    end

    def deploy_version
      @deploy_version ||= migration_config.fetch(:deploy_version, 0)
    end

    def migrate
      # execute migration
      # To-do: raise error message if the target version migration doesnt exist
      raise "Migration for version #{target_version} does not exist in #{migration_dir}"

      return if deploy_version == target_version
      # To-do: execute 'down' when the target version is smaller than deploy version 
      # To-do: execute 'up' when the target version is greater than deploy version 
      if deploy_version < target_version
        start_version = deploy_version
        goal_version = target_version
        up = true
      else
        start_version = target_version
        goal_version = deploy_version
        up = false 
      end
        
      migration_files.each do |file|
        current_version = file.split('/').last[0..3].to_i
        next if current_version < start_version || current_version > goal_version
        load file
        if up
          Migration.up
        else
          Migration.down
        end
      end
    end

    def run
      migrate
      super
    end
  end
end
