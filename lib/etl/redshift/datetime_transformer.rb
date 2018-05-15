require 'date'
require 'etl/transform/base'

module ETL::Redshift

  # handles converting Date and times to valid redshift
  # formated values
  class DateTimeTransformer < ::ETL::Transform::Base
    # column hash map is a hash of arrays of columns.
    def initialize(table_schema_lookup)
      super()
      @date_column_names = []
      @timestamp_column_names = []

      table_schema_lookup.each do |_table_name, table|
        table.columns.each_pair do |k, c|
          if c.type == :date
            @date_column_names << k.to_sym
          elsif c.type == :timestamp
            @timestamp_column_names << k.to_sym
          end
        end
      end
    end

    def transform(row)
      puts @date_column_names
      @date_column_names.each do |n|
        v = row[n]
        row[n] = v.strftime('%Y-%m-%d') if v.is_a?(Date)
      end
      @timestamp_column_names.each do |n|
        v = row[n]
        row[n] = v.strftime('%Y-%m-%d %H:%M:%S') if v.is_a?(Date) || v.is_a?(Time) || v.is_a?(DateTime)
      end
      puts "#{row.inspect}"
      row
    end
  end
end
