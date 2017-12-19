require 'tempfile'
require 'aws-sdk'
require 'csv'
require 'sequel'
require 'odbc'
require 'mixins/cached_logger'
require 'pathname'
require 'fileutils'
require 'connection_pool'
require_relative 'stl_load_error'

module ETL::Redshift
  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME = 'Amazon Redshift (x64)'.freeze

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :db, :region, :iam_role, :bucket, :delimiter, :row_columns_symbolized, :cache_table_schema_lookup, :tmp_dir, :stl_load_retries

    # when odbc driver is fully working the use redshift driver can
    # default to true
    def initialize(conn_params = {}, aws_params = {})
      @region = aws_params.fetch(:region)
      @bucket = aws_params.fetch(:s3_bucket)
      @iam_role = aws_params.fetch(:role_arn)
      @delimiter = "\u0001"

      # note the host is never specified as its part of the dsn name and for now that is hardcoded as 'MyRealRedshift'
      password = conn_params.fetch(:password)
      dsn = conn_params.fetch(:dsn, 'MyRealRedshift')
      user = conn_params.fetch(:username, nil) || conn_params.fetch(:user, '')
      raise 'No user was provided in the connection parameters' if user.empty?
      @odbc_conn_params = { database: dsn, password: password, user: user }
      ObjectSpace.define_finalizer(self, proc { disconnect })
      @row_columns_symbolized = true
      @cache_table_schema_lookup = true
      @cached_table_schemas = {}
      @tmp_dir = conn_params.fetch(:tmp_dir, '/tmp')
      @stl_load_retries = 10
      @slices_s3_files = ENV.fetch('ETL_REDSHIFT_SLICES', "5").to_i
    end

    def s3_resource
      s3_resource = Aws::S3::Resource.new(region: @region)
    end

    def disconnect
      db.disconnect
    end

    def db
      @db ||= Sequel.odbc(@odbc_conn_params)
    end

    def stl_load_errors(filter_opts)
      s3_file_name = filter_opts.fetch(:s3_filepath)
      query = 'Select * FROM stl_load_errors'
      query += " where filename = '#{s3_file_name}'" unless s3_file_name.nil?
      db.fetch(query).all
    end

    def stl_load_error_details(query_id)
      query = "Select * FROM STL_LOADERROR_DETAIL where query = '#{query_id}'"
      db.fetch(query).all
    end

    def execute_ddl(sql)
      log.debug("execute_ddl SQL: '#{sql}'")
      db.execute_ddl(sql)
    end

    def execute_dui(sql)
      log.debug("execute_dui SQL: '#{sql}'")
      db.execute_dui(sql)
    end

    def execute_insert(sql)
      log.debug("execute insert: SQL: '#{sql}'")
      db.execute_insert(sql)
    end

    def fetch(sql)
      log.debug("fetch SQL: '#{sql}'")
      db.fetch(sql)
    end

    def execute(sql)
      log.debug("execute SQL: '#{sql}'")
      db.execute(sql)
    end

    def drop_table(schema_name, table_name)
      sql = "DROP TABLE IF EXISTS #{schema_name}.#{table_name};"
      execute_ddl(sql)
    end

    def create_table(table)
      sql = table.create_table_sql
      execute_ddl(sql)
    end

    def table_schema(schema_name, table_name)
      cached_table = nil
      full_name = "#{schema_name}.#{table_name}"
      cached_table = @cached_table_schemas[full_name] if @cache_table_schema_lookup
      return cached_table unless cached_table.nil?
      execute("set search_path to #{schema_name}")
      information_schema_columns_sql = <<SQL
select * from pg_table_def
left Join information_schema.columns as i on i.table_schema = pg_table_def.schemaname and i.table_name = pg_table_def.tablename and i.column_name = pg_table_def."column"
where tablename = '#{table_name}' and schemaname = '#{schema_name}'
SQL
      columns_info = []
      fetch(information_schema_columns_sql).each { |v| columns_info << v }
      table_constraint_info_sql = <<SQL
With constraints as (
                SELECT
                  pg_namespace.nspname  AS schema_name,
                  pg_constraint.conname AS constraint_name,
                  conkey as primary_key_ordinals
                FROM pg_constraint
                  LEFT JOIN pg_namespace ON pg_constraint.connamespace = pg_namespace.OID
                WHERE pg_constraint.contype = 'p'
)
Select * FROM (
  SELECT *
  FROM information_schema.table_constraints
    JOIN constraints
      ON constraints.schema_name = information_schema.table_constraints.constraint_schema AND
         information_schema.table_constraints.constraint_name = constraints.constraint_name
) Where constraint_schema = '#{schema_name}' and table_name = '#{table_name}'
SQL
      pk_ordinals = []
      values = fetch(table_constraint_info_sql).all
      if !values.nil? && !values.empty?
        con_key = values[0].fetch(:primary_key_ordinals)
        split_keys = con_key.tr('{}', '').split(',')
        split_keys.each do |v|
          pk_ordinals << v.to_i
        end
      end

      fks_sql = <<SQL
SELECT * FROM (
  SELECT
    o.conname AS constraint_name,
    (SELECT nspname FROM pg_namespace WHERE oid=m.relnamespace) AS source_schema,
    m.relname AS source_table,
    (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = m.oid AND a.attnum = o.conkey[1] AND a.attisdropped = false) AS source_column,
    (SELECT nspname FROM pg_namespace WHERE oid=f.relnamespace) AS target_schema,
    f.relname AS target_table,
    (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = f.oid AND a.attnum = o.confkey[1] AND a.attisdropped = false) AS target_column
  FROM
    pg_constraint o LEFT JOIN pg_class c ON c.oid = o.conrelid
    LEFT JOIN pg_class f ON f.oid = o.confrelid LEFT JOIN pg_class m ON m.oid = o.conrelid
  WHERE
    o.contype = 'f' AND m.relname = '#{table_name}' AND o.conrelid IN (SELECT oid FROM pg_class c WHERE c.relkind = 'r')
) where source_schema = '#{schema_name}'
SQL
      fks = fetch(fks_sql).map
      table = ::ETL::Redshift::Table.from_schema(table_name, columns_info, pk_ordinals, fks)
      table.schema = schema_name
      @cached_table_schemas[table.name] = table if @cache_table_schema_lookup
      table
    end

    def columns(schema_name, table_name)
      sql = <<SQL
      SELECT "column", type FROM pg_table_def WHERE tablename = '#{table_name}' and schemaname = '#{schema_name}'
SQL
      fetch(sql)
    end

    def count_row_by_s3(destination)
      sql = <<SQL
        SELECT c.lines_scanned FROM stl_load_commits c, stl_query q WHERE filename LIKE 's3://#{destination}%'
        AND c.query = q.query AND trim(q.querytxt) NOT LIKE 'COPY ANALYZE%'
SQL
      results = fetch(sql)
      loaded_rows = 0
      results.each { |result| loaded_rows += result[:lines_scanned].to_i || 0 }
      loaded_rows
    end

    def unload_to_s3(query, destination)
      sql = <<SQL
        UNLOAD ('#{query}') TO 's3://#{destination}'
        IAM_ROLE '#{@iam_role}'
        DELIMITER '#{@delimiter}'
SQL
      execute(sql)
    end

    def copy_from_s3(table_name, s3_path, options = [])
      full_s3_path = "s3://#{s3_path}"
      additional_options = options.join("\n")
      sql = <<SQL
          COPY #{table_name}
          FROM '#{full_s3_path}'
          IAM_ROLE '#{@iam_role}'
          TIMEFORMAT AS 'auto'
          DATEFORMAT AS 'auto'
          DELIMITER '#{@delimiter}'
          REGION '#{@region}'
          ACCEPTINVCHARS
          #{additional_options}
SQL
      execute(sql)
    rescue => e
      if e.to_s.include? 'stl_load_errors'
        # should only be one error.
        load_error = stl_load_errors(s3_filepath: full_s3_path).first
        details = stl_load_error_details(load_error[:query])
        raise RedshiftSTLLoadError.new(load_error, details)
      end
      raise
    end

    def delete_object_from_s3(bucket, prefix, _session_name)
      s3 = Aws::S3::Client.new(region: @region)
      resp = s3.list_objects(bucket: bucket)
      keys = resp[:contents].select { |content| content.key.start_with? prefix }.map(&:key)

      keys.each { |key| s3.delete_object(bucket: bucket, key: key) }
    end

    def temp_file(table_name)
      # creating a daily file path so if the disk gets full its
      # easy to kiil all the days except the current.
      date_path = DateTime.now.strftime('%Y_%m_%d')
      dir_path = "#{@tmp_dir}/redshift/#{date_path}"
      FileUtils.makedirs(dir_path) unless Dir.exists?(dir_path)
      tmp_file = "#{dir_path}/#{table_name}_#{SecureRandom.hex(5)}"
      FileUtils.touch(tmp_file)
      tmp_file
    end

    # Upserts rows into the destintation tables based on rows
    # provided by the reader.
    def upsert_rows(reader, table_schemas_lookup, row_transformer, validator = nil, copy_options = [])
      add_rows(reader, table_schemas_lookup, row_transformer, validator,  copy_options, AddNewData.new('upsert'))
    end

    # Appends rows into the destintation tables based on rows
    # provided by the reader.
    def append_rows(reader, table_schemas_lookup, row_transformer, validator = nil, copy_options = [])
      add_rows(reader, table_schemas_lookup, row_transformer, validator,  copy_options, AddNewData.new('append'))
    end

    # adds rows into the destintation tables based on rows
    # provided by the reader and their add data type.
    def add_rows(reader, table_schemas_lookup, row_transformer, validator = nil, copy_options, add_new_data)
      # Remove new lines ensures that all row values have newlines removed.
      remove_new_lines = ::ETL::Transform::RemoveNewlines.new
      row_transformers = [remove_new_lines]
      row_transformers << row_transformer unless row_transformer.nil?

      csv_files = {}
      csv_file_paths = {}
      rows_processed_map = {}
      table_schemas_lookup.keys.each do |t|
        csv_file_paths[t] = temp_file(t)
        csv_files[t] = ::CSV.open(csv_file_paths[t], 'w', col_sep: @delimiter)
        rows_processed_map[t] = 0
      end

      begin
        reader.each_row do |row|
          values_lookup = transform_row(table_schemas_lookup, row_transformers, row)
          next if values_lookup.is_a? SkipRow

          values_lookup.each_pair do |table_name, row_arrays|
            tschema = table_schemas_lookup[table_name]
            row_arrays.each do |values_arr|
              csv_row = CSV::Row.new(tschema.columns.keys, values_arr)
              csv_files[table_name].add_row(csv_row)
              rows_processed_map[table_name] += 1
            end
          end
        end
      ensure
        table_schemas_lookup.each_pair do |t, tschema|
          if rows_processed_map[t] == 0
            log.debug("table #{t} has zero rows no upload required")
            next
          end

          csv_files[t].close
          local_file_path = csv_file_paths[t]
          tmp_table = create_staging_table(tschema.schema, t)
          copy_multiple_files_from_s3_with_retries(tmp_table, local_file_path, copy_options)

          full_table = "#{tschema.schema}.#{t}"
          where_id_join = ''
          tschema.primary_key.each do |pk|
            where_id_join = if where_id_join == ''
                              "where #{full_table}.#{pk} = #{tmp_table}.#{pk}"
                            else
                              "#{where_id_join} and #{full_table}.#{pk} = #{tmp_table}.#{pk}"
                            end
          end

          validator.validate(t, tmp_table, tschema) if validator
          add_sql = add_new_data.build_sql(tmp_table, full_table, where_id_join: where_id_join)
          execute(add_sql)
        end
      end
      highest_num_rows_processed = 0

      # might need to do something different but doing this for now.
      rows_processed_map.each_pair do |_key, value|
        highest_num_rows_processed = value if highest_num_rows_processed < value
      end
      highest_num_rows_processed
    end

    def build_error_file(file_prefix)
      # build local error file
      error_file_name = "#{file_prefix}_errors_#{SecureRandom.hex(5)}"
      date_path = DateTime.now.strftime('%Y_%m_%d')
      dir_path = "#{@tmp_dir}/redshift/#{date_path}"
      FileUtils.makedirs(dir_path) unless Dir.exists?(dir_path)
      error_file_path = "#{dir_path}/#{error_file_name}"
      s3_file_path = "s3://#{@bucket}/error_lines/#{date_path}/#{error_file_name}"
      FileUtils.touch(error_file_path)
      [error_file_name, s3_file_path]
    end

    def copy_from_s3_with_retries(tmp_table, local_file_path, options)
      error_file_path = nil
      s3_errors_file_path = nil
      stl_load_error_found = false
      retries = 0
      current_local_file = local_file_path
      files = [local_file_path]
      s3_files = []
      begin
        loop do
          stl_load_error_found = false
          begin
            s3_file_name = File.basename(current_local_file)
            s3_path = "#{@bucket}/#{s3_file_name}"
            s3_resource.bucket(@bucket).object(s3_file_name).upload_file(current_local_file)
            s3_files << s3_file_name
            copy_from_s3(tmp_table, s3_path, options)
            break
          rescue RedshiftSTLLoadError => e
            stl_load_error_found = true
            next_local_file = "#{local_file_path}_#{retries}"
            files << next_local_file
            found_error_row = self.class.remove_line_at(e.error_row[:line_number], current_local_file, next_local_file)
            ::File.delete(current_local_file)
            current_local_file = next_local_file

            # re-upload with removed line
            paths = build_error_file(tmp_table) if error_file_path.nil?
            error_file_path = paths[0] if error_file_path.nil?
            s3_errors_file_path = paths[1] if s3_errors_file_path.nil?
            open(error_file_path, 'a') do |f|
              f << found_error_row
            end
            if retries >= @stl_load_retries
              e.local_error_file = error_file_path unless error_file_path.nil?
              e.error_s3_file = s3_errors_file_path unless s3_errors_file_path.nil?
              raise e
            end
          ensure
            retries += 1
          end
        end
      ensure
        ::File.delete(current_local_file)

        # delete if no error
        s3_files.each { |f| s3_resource.bucket(@bucket).object(f).delete } if error_file_path.nil?
        unless error_file_path.nil?
          s3_resource.bucket(@bucket).object(s3_errors_file_path).upload_file(error_file_path)
          log.warning("There were errors uploading data, the following file in s3 contains the failed rows: #{s3_errors_file_path}")
        end
      end

      [error_file_path, s3_errors_file_path]
    end

    def copy_multiple_files_from_s3_with_retries(tmp_table, local_file_path, options)
      error = false 
      s3_errors_file_path = nil
      stl_load_error_found = false
      retries = 0
      current_local_file = local_file_path
      files = [local_file_path]
      s3_files = []

      begin
        s3_prefix, s3_multiple_files = upload_multiple_files_to_s3(current_local_file)
        s3_files += s3_multiple_files
        copy_from_s3(tmp_table, s3_prefix, options)
      rescue => e
        error = true
      ensure
        ::File.delete(current_local_file)
        # To-do: remove all local files
        remove_chunked_files(current_local_file)

        # delete if no error
        s3_files.each { |f| s3_resource.bucket(@bucket).object(f).delete } unless error
      end
    end

    def upload_multiple_files_to_s3(current_local_file_path, thread_count = 5)
      file_number = 0
      mutex       = Mutex.new
      threads     = []

      files = file_chunker(current_local_file_path)
      s3_files = []
      s3_path = ""

      thread_count.times do |i|
        threads[i] = Thread.new do
          until files.empty?
            mutex.synchronize do
              file_number += 1
              Thread.current['file_number'] = file_number
            end
            file = begin
                     files.pop
                   rescue
                     nil
                   end
            next unless file

            log.debug("[#{Thread.current['file_number']}/#{file}] uploading...")

            s3_file_name = File.basename(file)

            s3_resource.bucket(@bucket).object(s3_file_name).upload_file(file)
            s3_path = "#{@bucket}/#{s3_file_name}"
            s3_files << s3_path
          end
        end
      end
      threads.each(&:join)
      s3_prefix = s3_path.split('_')[0...-1].join("_")
      [s3_prefix, s3_files]
    end

    def file_chunker(file)
      prefix = file.split('.').first.split('/').last
      line_count = `wc -l "#{file}"`.strip.split(' ')[0].to_i
      max_line = line_count / @slices_s3_files
      outfilenum = 1
      files = []
      file_path = File.dirname(file)

      File.open(file, 'r') do |fh_in|
        until fh_in.eof?
          f_path = "#{file_path}/#{prefix}_#{outfilenum}.csv"
          File.open(f_path, 'w') do |fh_out|
            count = 0
            line = ''
            while (count <= max_line || outfilenum == @slices_s3_files) && !fh_in.eof?
              line = fh_in.readline
              fh_out << line
              count += 1
            end
            files << f_path
          end
          outfilenum += 1
        end
      end
      files
    end

    def remove_chunked_files(file)
      prefix = file.split('.').first.split('/').last
      file_path = File.dirname(file)
      system( "rm #{file_path}/#{prefix}_*.csv ")
    end

    def table_exists?(schema, table_name)
      sql = "SELECT count(*)  FROM pg_tables where schemaname = '#{schema}' and tablename = '#{table_name}'"
      result = fetch(sql).first
      result[:count] == 1
    end

    def create_staging_table(final_table_schema, final_table_name)
      tmp_table_name = final_table_name + SecureRandom.hex(5)
      # create temp table to add data to.
      tmp_table = ::ETL::Redshift::Table.new(tmp_table_name, temp: true, like: "#{final_table_schema}.#{final_table_name}")
      create_table(tmp_table)
      tmp_table_name
    end

    def transform_row(table_schemas_lookup, row_transformers, row)
      row_transformers.each do |t|
        row = t.transform(row)
      end

      return row if row.is_a? SkipRow

      is_named_rows = false
      raise "Row is not a Hash type, #{row.inspect}" unless row.is_a? Hash
      rows = if row.key?(table_schemas_lookup.keys[0])
               row
             else
               { table_schemas_lookup.keys[0] => row }
             end

      values_by_table = {}
      rows.each do |key, split_row|
        table_schema = table_schemas_lookup[key]

        split_rows = []
        split_rows = if split_row.is_a? Array
                       split_row
                     else
                       [split_row]
                     end

        split_rows.each do |r|
          values_arr = []
          table_schema.columns.keys.each do |c|
            values_arr << (r[c.to_sym] if r.key?(c.to_sym)) if @row_columns_symbolized
            values_arr << (r[c] if r.key?(c)) unless @row_columns_symbolized
          end
          if !values_by_table.key?(key)
            values_by_table[key] = [values_arr]
          else
            values_by_table[key] << values_arr
          end
        end
      end

      values_by_table
    end

    def self.remove_line_at(position, input_file, new_file)
      current_line_number = 1
      line_removed = ''
      open(input_file, 'r') do |f|
        open(new_file, 'w') do |f2|
          f.each_line do |line|
            if current_line_number == position
              line_removed = line
            else
              f2.write(line)
            end
            current_line_number += 1
          end
        end
      end
      line_removed
    end
  end

  class AddNewData
    def initialize(add_data_type)
      @add_data_type = add_data_type
    end

    def build_sql(tmp_table, destination_table_name, opts)
      sql = ''
      if @add_data_type == 'append'
        sql = <<SQL
begin transaction;
  insert into #{destination_table_name} select * from #{tmp_table};
end transaction;
SQL
      elsif @add_data_type == 'upsert'
        where_id_join = opts.fetch(:where_id_join)
        # Using recommended method to do upsert
        # http://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
        sql = <<SQL
begin transaction;
  delete from #{destination_table_name} using #{tmp_table} #{where_id_join};
  insert into #{destination_table_name} select * from #{tmp_table};
end transaction;
SQL
      else
        raise "Unknown add data type #{@add_data_type}"
      end
      sql
    end
  end

  # class used as sentinel to skip a row.
  class SkipRow
  end
end
