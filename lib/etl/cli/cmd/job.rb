require_relative '../command'

module ETL::Cli::Cmd
  class Job < ETL::Cli::Command

    class List < ETL::Cli::Command
      option ['-m', '--match'], "REGEX", "List only jobs matching regular expression",
             attribute_name: :regex do |r| /#{r}/ end

      def execute
        ETL.load_user_classes
        log.info("List of registered job IDs (classes):")
        ETL::Job::Manager.instance.job_classes.select do |id, klass|
          id =~ regex
        end.each do |id, klass|
          puts(" * #{id} (#{klass.name.to_s})")
        end
      end
    end

    class Run < ETL::Cli::Command
      parameter "JOB_ID", "ID of job we are running", required: true

      option ['-b', '--batch'], "BATCH", "Batch for the job in JSON or 'key1=value1;key2=value2' format", attribute_name: :batch_str
      option ['-q', '--queue'], :flag, "Queue the job instead of running now"
      option ['-m', '--match'], :flag, "Treat job ID as regular expression filter and run matching jobs"

      def execute
        ETL.load_user_classes

        klasses = job_classes(job_id, match?)
        if @batch_str
          if match?
            raise "Cannot pass batch with multiple jobs"
          end
          _, klass = klasses.first
          begin
            batch_factory = klass.batch_factory_class.new
            batch = batch_factory.parse!(@batch_str)
          rescue StandardError => ex
            raise ArgumentError, "Invalid batch value specified (#{ex.message})"
          end
          run_batch(job_id, batch)
        else
          # No batch string
          klasses.each do |id, klass|
            batch_factory = klass.batch_factory_class.new
            batch_factory.each do |batch|
              run_batch(id, batch)
            end
          end
        end
      end

      def job_classes(job_expr, fuzzy)
        if fuzzy
          ETL::Job::Manager.instance.job_classes.select do |id, klass|
            id =~ /#{job_expr}/
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
        puts payload
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

    subcommand 'list', 'Lists all jobs registered with ETL system', Job::List
    subcommand 'run', 'Runs (or enqueues) specified jobs + batches', Job::Run
  end
end
