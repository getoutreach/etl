module ETL
  module Redshift

    # Represents single data table in redshift
    class Table < ETL::Schema::Table
      attr_accessor :schema, :backup, :dist_key, :sort_keys, :encodings, :dist_style, :identity_key
      def self.compression_lookup
        {
          "NONE" => [],
          "RAW" => ["ALL"],  # no compression
          "AZ64" => ["SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "DATE", "TIMESTAMP", "TIMESTAMPTZ"],
          "BYTEDICT" => ["ALL" ], # supports all types except bool
          "DELTA"	=> ["SMALLINT", "INT", "BIGINT", "DATE", "TIMESTAMP", "DECIMAL"],
          "DELTA32K" => ["INT", "BIGINT", "DATE", "TIMESTAMP", "DECIMAL"],
          "LZO" => ["SMALLINT", "INTEGER","BIGINT", "DECIMAL", "CHAR", "VARCHAR", "DATE", "TIMESTAMP", "TIMESTAMPTZ"],
          "MOSTLYN8" => ["SMALLINT", "INT", "BIGINT", "DECIMAL"],
          "MOSTLYN16" => ["INT", "BIGINT", "DECIMAL"],
          "MOSTLYN32" => ["BIGINT", "DECIMAL"],
          "RUNLENGTH" => ["ALL"],
          "TEXT255" => ["VARCHAR"],
          "TEXT32K" => ["VARCHAR"],
          "Zstandard" => ["ALL"]
        }
      end

      def initialize(name = '', opts = {})
        super(name, opts)
        @dist_key = ''
        @sort_keys = []
        @encodings = {}
        @identity_key = {}
        @backup = opts.fetch(:backup, true)
        @dist_style = opts.fetch(:dist_style, '')
        @schema = 'public'
      end

      def set_distkey(column)
        @dist_key = column
      end

      def add_sortkey(column)
        @sort_keys.push(column)
      end

      def add_encoding(column, encoding)
        raise "Encoding #{encoding} unknown" unless self.class.compression_lookup.key?(encoding.upcase)
        @encodings[column] = encoding.downcase
      end

      def set_identity(column, seed = 1, step = 1)
        @identity_key = { column: column, seed: seed, step: step }
        columns[column.to_s].nullable = false
      end

      def smallint(name, &block)
        add_column(name, :smallint, nil, nil, &block)
      end

      def self.bool_convert(value, default)
        if value == 'NO'
          return false
        elsif value == 'YES'
          return true
        end
        default
      end

      def self.from_schema(table_name, columns_info, pk_info, fks)
        t = Table.new(table_name)
        pks = []
        sort_key = nil
        dist_key = nil
        columns_info.each do |col|
          col_name = col[:column_name]
          raise "Column name provided in the column info for table #{@name} is nil" if col_name.nil?
          ordinal_pos = col[:ordinal_position].to_i
          nullable = bool_convert(col[:is_nullable], true)
          data_type = col[:data_type]
          character_max = col[:character_maximum_length]
          width = col[:numeric_precision]
          scale = col[:numeric_scale]
          dist_key = col[:distkey]
          udt_name = col[:udt_name]
          sort_key = col[:sortkey]
          encoding = col[:encoding]

          pks << col_name.to_s if pk_info.include?(ordinal_pos)
          t.set_distkey(col_name) if dist_key
          t.add_sortkey(col_name) if sort_key != '0'
          t.add_encoding(col_name, encoding)
          data_type = 'varchar' if udt_name == 'varchar'
          data_type = 'char' if udt_name == 'char'
          data_type = 'nvarchar' if udt_name == 'nvarchar'

          if data_type == 'timestamp without time zone'
            data_type = 'timestamp'
          elsif data_type == 'timestamp with time zone'
            data_type = 'timestamptz'
          end

          type = case data_type
                 when 'smallint'
                   t.smallint(col_name)
                 when 'integer'
                   t.int(col_name)
                 when 'bigint'
                   t.bigint(col_name)
                 when 'double precision'
                   t.float(col_name)
                 when 'real'
                   t.float(col_name)
                 when 'float4'
                   t.float(col_name)
                 when 'float8'
                   t.float(col_name)
                 when 'boolean'
                   t.boolean(col_name)
                 when 'timestamp'
                   t.timestamp(col_name)
                when 'timestamptz'
                   t.timestamp(col_name)
                 when 'date'
                   t.date(col_name)
                 when 'text'
                   t.text(col_name)
                 when 'char'
                   t.char(col_name, character_max)
                 when 'nvarchar'
                   t.nvarchar(col_name, character_max)
                 when 'varchar'
                   t.varchar(col_name, character_max)
                 when 'numeric'
                   t.numeric(col_name, width, scale)
                 when nil
                   t.string(col_name)
                 else
                   raise "Unknown type: #{data_type} for col #{col_name}"
          end

          t.columns[col_name].nullable = nullable
          t.columns[col_name].ordinal_pos = ordinal_pos
        end

        unless fks.nil?
          fks.each do |fk|
            t.add_fk(fk[:source_column], fk[:target_table], fk[:target_column])
          end
        end

        t.primary_key = pks
        # putting in ordinal order as the csv will need to be in this order
        # this way keys are already ordered correctly.
        t.columns = t.columns.sort_by { |_key, value| value.ordinal_pos }.to_h
        t
      end

      def create_table_code
        code = "table = ::ETL::Redshift::Table.new('#{@name}')\n"
        @columns.each do |key, c|
          width = 'nil'
          width = c.width.to_s unless c.width.nil?
          precision = 'nil'
          width = c.precision.to_s unless c.precision.nil?
          code += "table.add_column('#{key}', '#{c.type}', #{width}, #{precision})\n"
          code += "table.add_encoding('#{key}', '#{@encodings[key]}')\n" if @encodings.key?(key)
        end
        code += "table.schema = '#{schema}'\n"
        code += "table.primary_key = [#{@primary_key.map { |k| "'#{k}'" }.join(',')}]\n" unless @primary_key.empty?
        code += "table.dist_key = '#{@dist_key}'\n" unless @dist_key.empty?
        code += "table.sort_key = [#{@sort_keys.map { |_k| 'k' }.join(',')}]\n" unless @sort_keys.empty?
        code += "table.dist_style = '#{@dist_style}'\n" unless @dist_style.empty?
        code += "table.backup = #{@backup}\n" if @backup == false
        code
      end

      def create_table_sql
        temp = ''

        sql = "CREATE TABLE IF NOT EXISTS #{@schema}.#{@name}" unless @temp
        sql = "CREATE TEMPORARY TABLE IF NOT EXISTS #{@name}" if @temp
        sql << " ( LIKE #{@like} )" unless @like.empty?

        column_declare_statements = ''
        type_ary = []
        columns.each do |name, column|
          column_type = col_type_str(column)
          column_statement = "\"#{name}\" #{column_type}"
          column_statement += " IDENTITY(#{@identity_key[:seed]}, #{@identity_key[:step]})" if !@identity_key.empty? && @identity_key[:column] == name.to_sym
          column_statement += ' NOT NULL' unless column.nullable
          column_statement += " encode #{@encodings[name]}" if @encodings.key?(name)
          column_statement += " REFERENCES #{column.fk[:table]}(#{column.fk[:column]})" unless column.fk.nil?
          type_ary << column_statement
        end

        unless @primary_key.empty?
          pks = @primary_key.join(',')
          type_ary << "PRIMARY KEY(#{pks})"
        end

        sql << "( #{type_ary.join(', ')} )" unless type_ary.empty?

        # backup is by default on if not specified
        sql << ' BACKUP NO' unless @backup

        sql << " DISTSTYLE #{@dist_style}" unless @dist_style.empty?

        sql << " DISTKEY(#{@dist_key})" unless @dist_key.empty?

        unless @sort_keys.empty?
          sks = @sort_keys.join(',')
          sql << " SORTKEY(#{sks})"
        end
        sql
      end

      def drop_table_sql
        sql = <<SQL
DROP TABLE IF EXISTS #{@name}
SQL
      end

      # Returns string that can be used as the database type given the
      # ETL::Schema::Column object
      def col_type_str(col)
        case col.type
        when :string
          'varchar(255)'
        when :date
          'date'
        when :timestamp
          'timestamp'
        when :timestampz
          'timestamp'
        when :numeric
          s = 'numeric'
          if !col.width.nil? || !col.precision.nil?
            s += '('
            s += col.width.nil? ? '0' : col.width.to_s
            s += ", #{col.precision}" unless col.precision.nil?
            s += ')'
          end
          s
        else
          # Allow other types to just flow through, which gives us a simple
          # way of supporting columns that are coming in through db reflection
          # even if we don't know what they are.
          col.type.to_s
        end
      end
    end
  end
end
