require_relative '../redshift/client'

module ETL::Migration
  # Base class for all jobs that are run
  class Redshift
    def initialize(client)
      @client = client
    end

    def up
      ""
    end

    def down
      ""
    end
  end
end
