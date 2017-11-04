require 'base64'

module ETL::Cache
  # Simple in-memory cache used primarily for surrogate
  # key lookups.
  class Base
    def self.hash_column_values(columns, row, symbolized)
      # Avoid hash computation of there is one key
      if columns.count == 1
        return row[columns[0].to_sym] if symbolized
        return row[columns[0]]
      end

      key_values = []
      columns.each do |c|
        key_values << row[c.to_sym] if symbolized
        key_values << row[c] unless symbolized
      end
      Base64.encode64(key_values.join('|'))
    end

    def initialize(columns, rows_symbolized = true)
      raise "columns nil" if columns.nil?
      raise "columns are not enumerable #{columns.inspect}" if !columns.is_a? Enumerable
      @columns = columns
      @rows_symbolized = rows_symbolized
    end

    def fill(reader)
      @row_lookup = {}
      reader.each do |row|
        key = self.class.hash_column_values(@columns, row, @rows_symbolized)
        if @row_lookup.key?(key)
          @row_lookup[key] << row
        else
          @row_lookup[key] = [row]
        end
      end
    end

    def find_rows(row)
      key = self.class.hash_column_values(@columns, row, @rows_symbolized)
      @row_lookup[key]
    end
  end
end
