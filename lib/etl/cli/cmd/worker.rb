require_relative '../command'
require 'etl/job/exec'

module ETL::Cli::Cmd
  # Class that handles processing jobs from the queue as a background process
  class Worker < ETL::Cli::Command
    # Starts the infinite loop that processes jobs from the queue
    def execute
      notifier = ::ETL::Slack::Notifier.create_instance("etl_worker")

      begin
        ETL.logger.debug("Starting the worker!")
        ETL.load_user_classes
        ETL.logger.debug("Classes loaded, waiting for work!")
        ETL.queue.handle_incoming_messages
      rescue StandardError => e
        ETL.logger.exception(e, Logger::DEBUG)
        notifier.notify_exception("loading jobs in etl worker failed:", e) unless notifier.nil?
        raise
      end
    end
  end
end
