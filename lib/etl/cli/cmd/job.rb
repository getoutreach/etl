require_relative '../command'
require_relative '../../slack/notifier'
require 'etl/job/exec'
require 'etl/models/job_run'
require 'etl/models/job_run_repository'
require 'terminal-table'

module ETL::Cli::Cmd
  class Job < ETL::Cli::Command
    class Status < ETL::Cli::Command
      option ['-s', '--start-time'], 'STARTTIME', 'Will query for any jobs starting for a specified start time. Optional parameter, defaults to 1 hour before now', attribute_name: :start_time
      option ['-t', '--type'], 'TYPE', 'Will query for any jobs with the specified status, defaults to running', attribute_name: :status
      option ['-e', '--exclude-properties'], 'PROPERTIES', 'Removes specified properties from complete list, default is nil', attribute_name: :excluded_properties
      option ['-i', '--include-properties'], 'PROPERTIES', 'Includes only specified properties, default is nil', attribute_name: :included_properties
      option ['-f', '--format'], 'FORMAT', "Format of the output, can be 'table' or 'json', defaults to 'json'", attribute_name: :format

      def execute
        @start_time = Time.now - 1 * 60 * 60 if @start_time.nil?
        @status = 'running' if @status.nil?
        @format = 'json' if @format.nil?

        return puts "The parameters '--exclude-properties' and '--include-properties' cannot be used at the same time" if !@excluded_properties.nil? && !@included_properties.nil?
        all_properties = ::ETL::Model::JobRun.properties

        properties = all_properties
        unless @included_properties.nil?
          properties = []
          @included_properties.split(',').each do |p|
            return puts "Included Property name '#{p}' not a property on JobRun, valid properties are '#{all_properties}' " unless all_properties.include?(p)
            properties << p
          end
        end

        unless @excluded_properties.nil?
          properties = []
          @excluded_properties = @excluded_properties.split(',')
          @excluded_properties.each do |p|
            return puts "Excluded Property name '#{p}' not a property on JobRun, valid properties are '#{all_properties}' " unless all_properties.include?(p)
          end
          all_properties.each do |p|
            properties << p unless @excluded_properties.include?(p)
          end
        end

        jrr_instance = ::ETL::Model::JobRunRepository.instance
        found_jobs = jrr_instance.find_by_status(@status, @start_time)
        view_hash_arr = []
        found_jobs.each do |job|
          hash = job.to_h
          view_hash = {}
          properties.each do |p|
            view_hash[p] = hash[p]
          end
          view_hash_arr << view_hash
        end
        if @format == 'table'
          table = Terminal::Table.new do |t|
            t.headings = properties
          end
          view_hash_arr.each do |v|
            table.add_row(v.values)
          end
          puts table
        else
          return puts '[]' if found_jobs.count.zero?
          puts JSON.pretty_generate(view_hash_arr)
        end
      end
    end

    class List < ETL::Cli::Command
      option ['-m', '--match'], 'REGEX', 'List only jobs matching regular expression',
             attribute_name: :regex, default: // do |r| /#{r}/ end

      def execute
        notifier = ::ETL::Slack::Notifier.create_instance('etl_list')

        begin
          ETL.load_user_classes
        rescue StandardError => e
          ETL.logger.exception(e, Logger::DEBUG) # don't lose the message!
          notifier.notify("Listing jobs failed: #{e}") unless notifier.nil?
          throw
        end
        dependencies_jobs = ETL::Job::Manager.instance.sorted_dependent_jobs
        d_jobs = dependencies_jobs.select { |id| id =~ regex }

        # Dependencies_jobs sorted by the order to be executed
        unless d_jobs.empty?
          output = " *** #{d_jobs.join(' ')}"
          puts(output)
          notifier.add_text_to_attachments(output) unless notifier.nil?
        end

        # Independent_jobs
        ETL::Job::Manager.instance.job_classes.select do |id, _klass|
          id =~ regex
        end.each do |id, klass|
          output = " * #{id} (#{klass.name})"
          puts(output) unless d_jobs.include? id
          notifier.add_text_to_attachments(output) unless notifier.nil?
        end
        notifier.notify('List ETL Jobs') unless notifier.nil?
      end
    end

    class Run < ETL::Cli::Command
      parameter 'JOB_ID', 'ID of job we are running', required: false, default: ''

      option ['-i', '--input'], 'INPUT', 'Provide a json file that has a list of job and batches to run', attribute_name: :input_file
      option ['-b', '--batch'], 'BATCH', "Batch for the job in JSON or 'key1=value1;key2=value2' format", attribute_name: :batch_str
      option ['-q', '--queue'], :flag, 'Queue the job instead of running now'
      option ['-m', '--match'], :flag, 'Treat job ID as regular expression filter and run matching jobs'

      def execute
        notifier = ::ETL::Slack::Notifier.create_instance('etl_run')
        begin
          ETL.load_user_classes
        rescue StandardError => e
          ETL.logger.exception(e, Logger::DEBUG) # don't lose the message!
          notifier.notify_exception('Loading jobs failed:', e) unless notifier.nil?
          throw
        end

        if @input_file && @batch_str
          raise ETL::UsageError, 'Invalid parameters, -i can not be used with -b'
        end
        if @input_file
          job_run_inputs = nil
          begin
            job_run_inputs = ::ETL::Model::JobRun.load_json_file(@input_file)
            puts "input: #{job_run_inputs}"
          rescue StandardError => e
            ETL.logger.exception(e, Logger::DEBUG)
            notifier.notify_exception("Loading jobs and batches from file #{@input_file} failed", e) unless notifier.nil?
          end
          job_run_inputs.each do |jr|
            id = jr['job_id']
            batch_str = jr['batch']
            klass = ETL::Job::Manager.instance.get_class(id)
            if klass.nil?
              raise ETL::UsageError, "Cannot find a job with specified job_id: '#{id}'"
            end

            batch = klass.batch_factory.parse!(batch_str)

            end_time_str = batch.to_h[:end_time]
            end_time = Time.parse(end_time_str).utc if !end_time_str.nil? && end_time_str.is_a?(String)

            current_time = Time.now.utc

            end_time_str = (end_time < current_time ? end_time_str : current_time.strftime("%Y-%m-%dT%H:%M:%S.%LZ"))

            batch_h = batch.to_h
            batch_h[:end_time] = end_time_str

            begin
              batch = klass.batch_factory.from_hash(batch_h)
            rescue StandardError => ex
              raise ETL::UsageError, "Invalid batch value specified (#{ex.message})"
            end

            begin
              run_batch(id, batch)
            rescue StandardError => e
              ETL.logger.exception(e, Logger::DEBUG)
              notifier.notify_exception("Running batch #{batch} failed", e) unless notifier.nil?
            end
          end
          return
        end

        klasses = job_classes(job_id, match?)

        if @batch_str
          if match?
            raise ETL::UsageError, 'Cannot pass batch with multiple jobs'
          end
          _, klass = klasses.fetch(0)
          begin
            batch = klass.batch_factory.parse!(@batch_str)
          rescue StandardError => ex
            raise ETL::UsageError, "Invalid batch value specified (#{ex.message})"
          end

          end_time_str = batch.to_h[:end_time]
          end_time = Time.parse(end_time_str) if !end_time_str.nil? && end_time_str.is_a?(String)

          current_time = Time.now.utc

          end_time_str = (end_time < current_time ? end_time_str : current_time.strftime("%Y-%m-%dT%H:%M:%S.%LZ"))

          batch_h = batch.to_h
          batch_h[:end_time] = end_time_str

          begin
            batch = klass.batch_factory.from_hash(batch_h)
          rescue StandardError => ex
            raise ETL::UsageError, "Invalid batch value specified (#{ex.message})"
          end

          run_batch(job_id, batch)
        else
          # No batch string
          klasses.each do |id, klass|
            begin
              klass.batch_factory.each do |batch|
                begin
                  run_batch(id, batch)
                rescue StandardError => e
                  ETL.logger.exception(e, Logger::DEBUG)
                  notifier.notify_exception("Running batch #{batch} failed", e) unless notifier.nil?
                end
              end
            rescue StandardError => e
              ETL.logger.exception(e, Logger::DEBUG) # don't lose the message!
              notifier.notify_exception("Running batch #{batch} failed", e) unless notifier.nil?
            end
          end
        end
      end

      def job_classes(job_expr, fuzzy)
        klasses = ETL::Job::Manager.instance.job_classes
        if klasses.empty?
          log.warn('No registered jobs')
          exit(0)
        end
        if fuzzy
          klasses.select do |id, _klass|
            id =~ /#{job_expr}/
          end.tap do |ks|
            raise "Found no job IDs matching '#{job_expr}'" if ks.empty?
          end
        else
          klass = ETL::Job::Manager.instance.get_class(job_expr)
          raise "Failed to find specified job ID '#{job_expr}'" unless klass
          [[job_expr, klass]]
        end
      end

      # runs the specified batch
      def run_batch(id, batch)
        run_payload(ETL::Queue::Payload.new(id, batch))
      end

      # enqueues or runs specified payload based on param setting
      def run_payload(payload)
        if queue?
          log.info("Enqueuing #{payload}")
          ETL.queue.enqueue(payload)
        else
          log.info("Running #{payload}")
          result = ETL::Job::Exec.new(payload).run
          if result.success?
            log.info("SUCCESS: #{result.message}")
          else
            log.error(result.message)
          end
        end
      end
    end
    subcommand 'status', 'Lists all batches currently in-progress', Job::Status
    subcommand 'list', 'Lists all jobs registered with ETL system', Job::List
    subcommand 'run', 'Runs (or enqueues) specified jobs + batches', Job::Run
  end
end
