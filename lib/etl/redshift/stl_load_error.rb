require 'json'

module ETL
  module Redshift
    class RedshiftSTLLoadError < StandardError
      attr_accessor :error_row, :detail_rows, :error_s3_file, :local_error_file
      def initialize(query_id, error_row, detail_rows)
        @query_id = query_id
        @error_row = error_row
        @detail_rows = detail_rows
      end

      def message
        "STL Load error on query '#{@query_id}': Reason: '#{error_row.to_json}'"
      end
    end
  end
end
