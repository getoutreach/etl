module ETL::Input
  class Pagerduty < ::ETL::Input::Base
    API_ROOT = "https://api.pagerduty.com"

    attr_accessor :api_token, :fields

    def initialize(api_token:, fields:)
      @api_token = api_token
      @fields = fields.map(&:to_s)
    end

    def request_incidents
      uri = URI("#{API_ROOT}/incidents")

      request = Net::HTTP::Get.new(uri)
      request['Authorization'] = "Token token=#{api_token}"
      request['Accept'] = 'application/vnd.pagerduty+json;version=2'

      response = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
        http.request(request)
      end
      JSON.parse(response.body)
    end

    def each_row(batch = ETL::Batch.new)
      incidents = request_incidents.fetch('incidents')
      incidents.each do |obj|
        # Use fetch to avoid nils
        yield Hash[fields.map { |k| [k, obj.fetch(k)] }]
      end
    end
  end
end
