require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Redshift
    def initialize
      conn_params = ETL.config.redshift.fetch(:etl, { host: "localhost", port: 5439, user: "masteruser", password: "password" })
      aws_params = ETL.config.aws.fetch(:etl)
      @client = ::ETL::Redshift::Client.new(conn_params, aws_params)
      ObjectSpace.define_finalizer(self, proc { @client.disconnect })
    end

    def up
      ""
    end

    def down
      ""
    end
  end
end
