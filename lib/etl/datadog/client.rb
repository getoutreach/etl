require 'rubygems'
require 'dogapi'

module ETL::Datadog
  class Client
    attr_accessor :host_name

    def self.create_instance
      ::ETL::Datadog::Client.new
    end

    def initialize
      api_key = ENV['DATADOG_API_KEY']
      @host_name = ENV['ETL_DATADOG_URL']
      @client = if api_key.nil?
                  nil
                else
                  Dogapi::Client.new(api_key)
                end
    end

    def send_event(message, message_title, alert_type, tags = ['legacy_etl'])
      unless @host_name.nil?
        @client.emit_event(Dogapi::Event.new(message,
                                             msg_title: message_title,
                                             alert_type: alert_type,
                                             tags: tags),
                           host: @host_name)
      end
    end
  end
end
