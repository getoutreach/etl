module ETL::Job

  # Base class for all migratable jobs that are run
  class MigratableBase < Base

    def initialize(b)
      super(b)
    end

    def migration_files
      Dir["#{migration_dir}/#{id}_*.rb"]
    end

    def migration_dir
      Dir.pwd
    end

    def target_version
      1
    end

    def set_schema_version(version)
      ENV["#{env_name}"] = version.to_s
    end

    def get_schema_version
      ENV["#{env_name}"]
    end

    def env_name
      "#{id.upcase}_SCHEMA_VERSION"
    end

    def deploy_version
      @deploy_version ||= begin
        version = get_schema_version
        raise "#{env_name} is not set" unless version
        version.to_i
      end
    end

    def version_migration_file_exists?(goal_version)
      file_path = version_migration_file_path(goal_version)
      migration_files.include? file_path
    end

    def version_migration_file_path(goal_version)
      "#{migration_dir}/#{id}_#{ETL::StringUtil.digit_str(goal_version)}.rb"
    end

    def migrate(client)
      # execute migration
      return if deploy_version == target_version
      move_direction = 0
      # To-do: execute 'down' when the target version is smaller than deploy version
      # To-do: execute 'up' when the target version is greater than deploy version
      if deploy_version < target_version
        start_version = deploy_version + 1
        goal_version = target_version
        move_dirction = 1
      else
        start_version = deploy_version
        goal_version = target_version + 1
        move_direction = -1
      end

      # Raise error message if the target version migration doesnt exist
      raise "Migration for version #{goal_version} does not exist in #{migration_dir}" unless version_migration_file_exists?(goal_version)

      current_version = start_version
      while true
        version = ETL::StringUtil.digit_str(current_version)
        load version_migration_file_path(version)
        clazz = "Migration::#{ETL::StringUtil::snake_to_camel(file)}".split('::').inject(Object) {|o,c| o.const_get c}
        m = clazz.new(client)
        if move_direction == 1
          m.up
        else
          m.down
        end
        break if current_version == goal_version
        current_version += move
      end
      set_schema_version(target_version)
    end

    def run
      # Call with your client
      migrate("client")
      super
    end
  end
end
