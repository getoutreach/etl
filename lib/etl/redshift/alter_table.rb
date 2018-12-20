module ETL
  module Redshift
    # AlterTable will allow changes in columns of redshift.
    class AlterTable
      def initialize(schema_name, table_name)
        @schema_name = schema_name
        @table_name = table_name
        @alter_table_commands = []
      end

      def add_column(name, type, default = nil, encoding = nil, not_null = false)
        command = "ALTER TABLE #{@schema_name}.#{@table_name} add column #{name} #{type}"
        command = "#{command} DEFAULT #{default}" unless default.nil?
        command = "#{command} ENCODE #{encoding}" unless encoding.nil?
        command = "#{command} NOT NULL" if not_null # only write if not null
        @alter_table_commands << "#{command};"
      end

      def copy_column(new_col, old_col)
        command = "UPDATE #{@schema_name}.#{@table_name} SET #{new_col} = #{old_col};"
        @alter_table_commands << command
      end

      def drop_column(name)
        @alter_table_commands << "ALTER TABLE #{@schema_name}.#{@table_name} drop column #{name};"
      end

      def rename_column(from_name, to_name)
        @alter_table_commands << "ALTER TABLE #{@schema_name}.#{@table_name} rename column #{from_name} TO #{to_name};"
      end

      def execute(client)
        client.execute(build_sql)
      end

      def build_sql
        sql = <<SQL
  begin transaction;
  #{@alter_table_commands.join("\n\t")}
  end transaction;
SQL
        sql
      end
    end
  end
end
