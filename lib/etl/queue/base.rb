require 'etl/queue/payload.rb'
require 'etl/job/exec'
require 'etl/mixins/cached_logger'

module ETL::Queue

  # Base class that defines the interface our work queues need
  class Base
    include ETL::CachedLogger
    attr_accessor :dequeue_pauser

    # Starts async processing of the queue. When an element is read off the
    # queue the |message_info, payload| is passed to block.
    def process_async(&block)
    end

    # Places the specified payload onto the queue for processing by a worker.
    def enqueue(payload)
    end

    # Purges all jobs from the queue
    def purge
    end

    # Returns number of messages in the queue
    def message_count
      0
    end

    # Acknowledges that the specified message d
    def ack(msg_info)
    end

    def pause_dequeueing?
      return false if @dequeue_pauser.nil?
      @dequeue_pauser.pause_dequeueing?
    end

    def dequeue_pause_wait_seconds
      return 60 if @dequeue_pauser.nil?
      @dequeue_pauser.wait_seconds
    end

    def pause_work_if_dequeuing_paused
      while pause_dequeueing?
        ETL.logger.debug("Pause execution of messages")
        sleep dequeue_pause_wait_seconds
      end
    end

    def handle_incoming_messages
      pause_work_if_dequeuing_paused
 
      process_async do |message_info, payload|
        # note not acking on non user exceptions because we want the messages to be re-queued if process was killed though a sigterm
        begin
          log.debug("Payload: #{payload.to_s}")
          exec = ETL::Job::Exec.new(payload)
          exec.run unless exec.nil?
          ETL.queue.ack(message_info)
        rescue StandardError => ex
          # Log and ignore all exceptions. We want other jobs in the queue
          # to still process even though this one is skipped.
          log.exception(ex)
          ETL.queue.ack(message_info)
        end

        pause_work_if_dequeuing_paused
      end

      # Just sleep indefinitely so the program doesn't end. This doesn't pause the
      # above block.
      while true
        sleep(10)
      end
    end
  end
end
