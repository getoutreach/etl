odule ETL::Redshift

  # handles converting Date and times to valid redshift
  # formated values
  class DateTimeTransformer < Base

    # column hash map is a hash of arrays of columns.
    def initialize(table_schema_lookup)
      super()
      @date_column_names = []
      @timestamp_column_names = []

      table_schema_lookup.each_pair do |_table_name, table|
        table.columns.each_pair do |k, v|
          v = table.col_type_str(c)
          if v == 'date'
            @date_column_names << k
          elsif v == 'timestamp'
            @timestamp_column_names << k
          end
        end
      end
    end

    def transform(row)
      @date_column_names.each do |n|
        if row[n].is_a?(Date)
          row[n] = Date.strptime(row[n], '%Y-%m-%d')
        end
      end
      @timestamp_column_names.each do |n|
        if row[n].is_a?(Date)
          row[n] = Time.strptime(row[n], '%Y-%m-%d %H:%M:%S')
        end
      end
    end
  end
end
