require_relative '../redshift/client'
require_relative '../redshift/table'
require_relative '../redshift/date_table_id_augmenter'
require_relative '../redshift/column_value_augmenter'

module ETL::Output

  # new Redshift outputter that defers logic to client
  class Redshift2 < Base
    include ETL::CachedLogger
    attr_accessor :now_generator, :id_generator, :delimiter, :pre_transformer
    
    def initialize(client, main_table_name, optional_history_table_name, surrogate_key, natural_keys, scd_columns)
      super()
      @client = client
      @delimiter = '\001'
      @surrogate_key = surrogate_key
      @natural_keys = natural_keys
      @main_table_name = main_table_name
      @history_table_name = optional_history_table_name
      @scd_columns = scd_columns
      @id_generator = id_generator || IDUUIDGenerator.new
      @now_generator = ::ETL::Output::CurrentDateTimeGenerator.new
      raise "main_table_name must not be empty" if @main_table_name.empty?
    end

    def build_transformer(table_schemas_lookup)
      transformers = []
      transformers << @pre_transformer if !@pre_transformer.nil?

      if @history_table_name.nil?
        # TODO: Will need to make improvements here but should work for now.
        transformers << ::ETL::Redshift::DateTableIDAugmenter.new(table_schemas_lookup.values)
      else
        main_table_schema = table_schemas_lookup[@main_table_name]
        history_table_schema = table_schemas_lookup[@history_table_name]
        transformers << transformer = ::ETL::Output::DataHistoryRowTransformer.new(@client, @scd_columns, @natural_keys, @id_generator, main_table_schema, history_table_schema, @now_generator)
      end
      MultiTransformer.new(transformers)
    end

    # Runs the ETL job
    def run_internal
      main_table_schema = @client.table_schema(@main_table_name)
      table_schemas_lookup = { main_table_schema.name => main_table_schema }
      if !@history_table_name.nil?
        history_table_schema = @client.table_schema(@history_table_name)
        table_schemas_lookup[history_table_schema.name] = history_table_schema
      end

      transformer = build_transformer(table_schemas_lookup)

      rows_processed = @client.upsert_rows(reader, table_schemas_lookup, transformer)
      msg = "Processed #{rows_processed} input rows for #{main_table_schema.name}"
      ::ETL::Job::Result.success(rows_processed, msg)
    end
  end

  class MultiTransformer
    def initialize(transformers = [])
      @transformers = transformers
    end

    def transform(row)
      @transformers.each do |t|
        row = t.transform(row)
      end
      row
    end
  end

  class DataHistoryRowTransformer
    def initialize(client, scd_columns, natural_keys, id_generator, main_table_schema, history_table_schema, now_generator)
      raise "client cannot be nil" if client.nil?
      raise "natural_keys cannot be nil" if natural_keys.nil?
      raise "scd columns cannot be nil" if scd_columns.nil?
      raise "id_generator cannot be nil" if id_generator.nil?
      raise "main_table_schema cannot be nil" if main_table_schema.nil?
      raise "history_table_schema cannot be nil" if history_table_schema.nil?
      raise "main_table_schema #{main_table_schema.name} must have one primary key" if main_table_schema.primary_key.count != 1

      @surrogate_key = main_table_schema.primary_key[0]
      @scd_columns = scd_columns
      @natural_keys = natural_keys
      @id_generator = id_generator
      @main_table_schema = main_table_schema
      @history_table_schema = history_table_schema
      @client = client
      @now_generator = now_generator
    end

    def transform(row)
      named_rows = {}
      # Conditionally creates more named rows based on the
      # change in the row
      table_schemas_lookup = { @main_table_schema.name => @main_table_schema }
      table_schemas_lookup[@history_table_schema.name] = @history_table_schema if !@history_table_schema.nil?

      date_table_id_augmenter = ::ETL::Redshift::DateTableIDAugmenter.new(table_schemas_lookup.values)
      row = date_table_id_augmenter.transform(row)
      column_augmenter = ::ETL::Redshift::ColumnValueAugmenter.new(@client, @history_table_schema, @surrogate_key, @natural_keys, @scd_columns, nil)
      row = column_augmenter.transform(row)

      row_splitter = ::ETL::Transform::SplitRow.SplitByTableSchemas(table_schemas_lookup.values)
      named_rows = row_splitter.transform(row)

      raise "data table #{@main_table_schema.name} must only have one pk" if !@main_table_schema.primary_key.count == 1
      raise "history table #{@history_table_schema.name} must only have one pk" if !@history_table_schema.primary_key.count == 1
      main_table_surrogate_key = @main_table_schema.primary_key[0]
      history_table_key = @history_table_schema.primary_key[0]
      # If no surrogate key exists then none was found so its new.
      if !named_rows[@main_table_schema.name].has_key?(main_table_surrogate_key)
        # This is a new row so need the following
        #    a. new row for main table
        #    b. new row for history table
        new_history_id = @id_generator.generate_id
        new_surrogate_id = @id_generator.generate_id
        named_rows[@main_table_schema.name][main_table_surrogate_key] = new_surrogate_id
        named_rows[@history_table_schema.name][main_table_surrogate_key] = new_surrogate_id
        named_rows[@history_table_schema.name][history_table_key] = new_history_id
        named_rows[@history_table_schema.name]["current"] = true
        named_rows[@history_table_schema.name]["created_at"] = @now_generator.now
      else
        # an existing row has changed
        scd_changed = false
        @scd_columns.each do |scd|
          if row["old_#{scd}"] != row[scd]
            scd_changed = true
            break
          end
        end

        if scd_changed
          # When a slowly changing dimension hash changed the following needs to occur
            #    a. update current history row to not be current and add end_date
            #    b. add new row for history with the start time to now and it being current.
            existing_history_row = named_rows[@history_table_schema.name]
            new_history_row = existing_history_row.clone
            existing_history_row = named_rows[@history_table_schema.name]
            existing_history_row["current"] = false
            existing_history_row["ended_at"] = @now_generator.now

            new_history_id = @id_generator.generate_id
            new_history_row[history_table_key] = new_history_id
            new_history_row["current"] = true
            new_history_row["created_at"] = @now_generator.now
            named_rows[@history_table_schema.name] = [existing_history_row, new_history_row]
        else
          named_rows.delete(@history_table_schema.name)
        end
      end
      named_rows
    end
  end

  class CurrentDateTimeGenerator
    def now
      DateTime.now
    end
  end

  class IDUUIDGenerator
    def generate_id
      SecureRandom.uuid
    end
  end
end
