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
        row = {}
        @detail_rows.each do |dr|
          row[dr[:colname].strip] = dr[:value].to_s.strip
        end
        "STL Load error on query '#{@query_id}': Reason: 'From #{@error_row[:err_reason].strip}', s3_location: #{@error_row[:filename]} LineNumber: #{@error_row[:line_number]}, Position: #{@error_row[:position]}, Rawline '#{error_row[:raw_line].strip}', \nparsed row: '#{row.to_s}'"
      end
    end
  end
end
