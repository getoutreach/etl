module ETL

  module Redshift

    # Represents single data table in redshift
    class Table < ETL::Schema::Table
      attr_accessor :backup, :dist_key, :sort_keys, :dist_style

      def initialize(name="", opts = {})
        super(name, opts)
        @dist_key = ""
        @sort_keys = []
        @identity_key = {}
        @backup = opts.fetch(:backup, true)
        @dist_style = opts.fetch(:dist_style, '')
      end

      def set_distkey(column)
        @dist_key = column
      end

      def add_sortkey(column)
        @sort_keys.push(column)
      end

      def set_identity(column, seed=1, step=1)
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

      def self.from_schema(table_name, columns_info, pk_info)
        t = Table.new(table_name)
        pks = []
        sort_key = nil
        dist_key = nil
        columns_info.each do |col|
          col_name = col["column_name"]
          ordinal_pos = col["ordinal_position"].to_i
          nullable = bool_convert(col["is_nullable"], true)
          data_type = col["data_type"]
          character_max = col["character_maximum_length"]
          width = col["numeric_precision"]
          scale = col["numeric_scale"]
          dist_key = col["distkey"]
          udt_name = col["udt_name"]
          sort_key = col["sortkey"]

          pks << col_name.to_s if pk_info.include?(ordinal_pos)
          t.set_distkey(col_name) if dist_key
          t.add_sortkey(col_name) if sort_key != "0"

          data_type = "varchar" if udt_name == "varchar"

          if data_type == "timestamp without time zone"
            data_type = "timestamp"
          elsif data_type == "timestamp with time zone"
            data_type = "timestamptz"
          end

          type = case data_type
          when "smallint"
            t.smallint(col_name)
          when "integer"
            t.int(col_name)
          when "bigint"
            t.bigint(col_name)
          when "double precision"
            t.float(col_name)
          when "real"
            t.float(col_name)
          when "float4"
            t.float(col_name)
          when "float8"
            t.float(col_name)
          when "boolean"
            t.boolean(col_name)
          when "timestamp"
            t.timestamp(col_name)
          when "timestamptz"
            t.timestamp(col_name)
          when "date"
            t.date(col_name)
          when "text"
            t.text(col_name)
          when "varchar"
            t.varchar(col_name, character_max)
          when "numeric"
            t.numeric(col_name, width, scale)
          when nil
            t.string(col_name)
          else
            raise "Unknown type: #{data_type} for col #{col_name}"
          end

          t.columns[col_name].nullable = nullable
          t.columns[col_name].ordinal_pos = ordinal_pos
        end

        t.primary_key = pks
        # putting in ordinal order as the csv will need to be in this order
        # this way keys are already ordered correctly.
        t.columns = t.columns.sort_by{|_key, value| value.ordinal_pos}.to_h
        return t
      end

      def create_table_sql(using_redshift_odbc_driver = true)
        temp =""
        temp = if @temp
                 " TEMPORARY"
               end

        sql = "CREATE#{temp} TABLE IF NOT EXISTS #{@name}"
        if !@like.empty?
          sql << " ( LIKE #{@like} )"
        end

        column_declare_statements = ""
        type_ary = []
        columns.each do |name, column|
          column_type = col_type_str(column)
          column_statement = "\"#{name}\" #{column_type}"
          column_statement += " IDENTITY(#{@identity_key[:seed]}, #{@identity_key[:step]})" if !@identity_key.empty? && @identity_key[:column] == name.to_sym
          column_statement += " NOT NULL" if !column.nullable
          type_ary << column_statement
        end

        if @primary_key.length > 0
          pks = @primary_key.join(",")
          type_ary << "PRIMARY KEY(#{pks})"
        end

        if type_ary.length > 0
          sql << "( #{type_ary.join(', ')} )"
        end

        # If the sql provider doesn't support redshift specific
        # create table pieces don't add them in.
        if using_redshift_odbc_driver then
          # backup is by default on if not specified
          if !@backup
            sql << " BACKUP NO"
          end

          if !@dist_key.empty?
            sql << " DISTKEY(#{@dist_key})"
          end

          if @sort_keys.length > 0
            sks = @sort_keys.join(",")
            sql << " SORTKEY(#{sks})"
          end

          if !@dist_style.empty?
            sql << " DISTSTYLE #{@dist_style}"
          end
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
            "varchar(255)"
          when :date
            "date"
          when :timestamp
            "timestamp"
          when :numeric
            s = "numeric"
            if !col.width.nil? || !col.precision.nil?
              s += "("
              s += col.width.nil? ? "0" : col.width.to_s()
              if !col.precision.nil?
                s += ", #{col.precision}"
              end
              s += ")"
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

