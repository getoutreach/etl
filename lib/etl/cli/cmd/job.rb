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
      option ['-m', '--match'], "REGEX", "Run only jobs matching regular expression"
      
      def execute
        ETL.load_user_classes
        
        if match
          if @batch_str
            raise "Cannot pass batch with multiple jobs"
          end

          ETL::Job::Manager.instance.job_classes.select do |id, klass|
            id =~ /#{regex}/
          end.each do |id, klass|
            batch_factory = klass.batch_factory_class.new
            batch_factory.each do |b|
              run_batch(b)
            end
          end
        else
          if @batch_str # user-specified batch
            begin
              batch = batch_factory.parse!(@batch_str)
            rescue StandardError => ex
              raise ArgumentError, "Invalid batch value specified (#{ex.message})"
            end
            run_batch(batch)
          else # need to generate the batch(es) from the job
            batch_factory.each do |b|
              run_batch(b)
            end
          end
        end
      end

      def run_multiple
        ETL.load_user_classes
      end
      
      def job_class
        @job_class ||= ETL::Job::Manager.instance.get_class(job_id)
        raise "Failed to find specified job ID '#{job_id}'" unless @job_class
        @job_class
      end
      
      def batch_factory
        @batch_factory ||= job_class.batch_factory_class.new
      end
      
      # runs the specified batch
      def run_batch(b)
        run_payload(ETL::Queue::Payload.new(job_id, b))
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
    
    subcommand 'list', 'Lists all jobs registered with ETL system', Job::List
    subcommand 'run', 'Runs (or enqueues) specified jobs + batches', Job::Run
    subcommand 'enqueue-all', 'Enqueues multiple jobs matching a tag', Job::EnqueueAll
  end
end
