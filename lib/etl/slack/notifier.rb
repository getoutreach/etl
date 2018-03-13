require 'slack-notifier'
require_relative '../util/logger'

module ETL::Slack
  class << self
    attr_accessor :notifier_mutex
  end

  class Notifier
    attr_accessor :attachments, :error_occurred_at_time, :wait_time_seconds

    def self.create_instance(id)
      notifier ||= begin
        if ETL.config.core[:slack]
          slack_config = ETL.config.core[:slack]
          if slack_config[:url] && slack_config[:channel] && id
            ::ETL::Slack::Notifier.new(slack_config[:url], slack_config[:channel], id)
          end
        end
      end
      notifier
    end

    def initialize(webhook_url, channel, username)
      @start_wait_time_seconds = 30
      @wait_time_seconds = 30
      @notifier = Slack::Notifier.new(webhook_url, channel: channel, username: username)
      @attachments = []
      ETL::Slack.notifier_mutex = Mutex.new if ETL::Slack.notifier_mutex.nil?
    end

    def notify_exception(message, exception, icon_emoji: ":beetle:", attachments: @attachments)
      msg = ::ETL::Logger.create_exception_message(exception)
      ping "#{message}: #{msg}", icon_emoji: icon_emoji, attachments: attachments
    end

    def notify(message, icon_emoji: ":beetle:", attachments: @attachments)
      ping message, icon_emoji: icon_emoji, attachments: attachments
    end

    def set_color(color)
      if @attachments.empty?
        @attachments = [{ color: color }]
      else
        @attachments[0][:color] = color
      end
    end

    def add_text_to_attachments(txt)
      if @attachments.empty?
        @attachments = [{ text: txt }]
      else
        if @attachments[0].include? :text
          @attachments[0][:text] += "\n" + txt
        else
          @attachments[0][:text] = txt
        end
      end
    end

    def add_field_to_attachments(field)
      if @attachments.empty?
        @attachments = [{ fields: [ field ] }]
      else
        if @attachments[0].include? :fields
          @attachments[0][:fields].push(field)
        else
          @attachments[0][:fields] = [field]
        end
      end
    end

    private
      def ping(msg, **args)
        ETL::Slack.notifier_mutex.synchronize do
          begin
            if should_ping?(msg)
              @notifier.ping msg, args
              @wait_time_seconds = @start_wait_time_seconds
            end
          rescue Slack::Notifier::APIError => api_error
            ETL.logger.exception(api_error)
            if api_error.message.include?('rate_limit')
              @error_occurred_at_time = Time.now @error_occurred_at_time.nil?
              # after each failure doubling the time to wait to backoff
              @wait_time_seconds = @wait_time_seconds + rand(@wait_time_seconds-5..@wait_time_seconds+5)
            end
          rescue StandardError => ex
            ETL.logger.exception(ex)
          end
        end
      end

      def should_ping?(msg)
        return true if @error_occurred_at_time.nil?
        elapsed_seconds = Time.now - @error_occurred_at_time
        return true if elapsed_seconds > @wait_time_seconds
        ETL.logger.debug("Skipping slack output: '#{msg}' due to rate limiting")
        false
      end
  end
end
