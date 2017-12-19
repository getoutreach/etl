require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/core'

def create_test_file
  File.open("client_test.csv", "w+") do |f|
    20.times do |i|
      f.write("abc|def|ghi#{i}\n")
    end
  end
end

def delete_test_file
  system( "rm client_test*.csv ")
end

def create_test_table(client)
 client.drop_table('public', 'client_test')
 create_table = <<SQL
    create table client_test (
      col1 varchar(8),
      col2 varchar(8),
      col3 varchar(8) );
SQL
  client.execute(create_table)
end

RSpec.describe 'redshift' do
  context 'client testing' do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test]) }
    let(:table_name) { 'test_table_1' }
    let(:bucket) { ETL.config.aws[:etl][:s3_bucket] }
    let(:random_key) { [*('a'..'z'), *('0'..'9')].sample(10).join }
    let(:s3_destination) { "#{bucket}/#{table_name}_#{random_key}" }
    it 'connect and create and delete a table' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
      client2.drop_table('public', table_name)
      other_table = ETL::Redshift::Table.new(:other_table)
      other_table.int('id')
      other_table.add_primarykey('id')

      table = ETL::Redshift::Table.new(table_name, backup: false, dist_style: 'All')
      table.string('name')
      table.int('id')
      table.int('fk_id')
      table.add_fk(:fk_id, 'other_table', 'id')
      table.add_primarykey('id')

      client2.create_table(other_table)
      client2.create_table(table)

      found_table = client2.table_schema('public', table_name)
      expect(found_table.fks).to eq(['fk_id'])
      expect(found_table.columns['fk_id'].fk).to eq(column: 'id', table: 'other_table')
      client2.drop_table('public', table_name)
      client2.drop_table('public', 'other_table')
      client2.disconnect
    end

    it 'table_exists' do
      expect(client.table_exists?("pg_catalog", "pg_namespace")).to eq(true)
      expect(client.table_exists?("pg_catalog", "non_existent_table")).to eq(false)
    end

    it 'get table schema' do
      client.execute('DROP SCHEMA IF EXISTS other_schema CASCADE')
      client.execute('CREATE SCHEMA other_schema')
      sql = <<SQL
  create table other_schema.#{table_name} (
    day timestamp,
    day2 timestamptz,
    id integer,
    test varchar(22),
    num numeric(5,2),
    f1 float8,
    f2 float4,
    large_int bigint,
    small_int smallint,
    PRIMARY KEY(id) );
SQL
      client.execute(sql)
      rows = []
      schema = client.table_schema('other_schema', table_name)

      expect(schema.columns.keys).to eq(%w[day day2 id test num f1 f2 large_int small_int])
      expect(schema.primary_key).to eq(['id'])
      expect(schema.schema).to eq('other_schema')
    end

    it 'get table columns' do
      client.drop_table('public', table_name)
      sql = <<SQL
  create table #{table_name} (
    day timestamp);
SQL
      client.execute(sql)
      rows = []
      r = client.columns('public', table_name).each do |r|
        rows << r
      end
      expect(rows).to eq([{:column=>"day", :type=>"timestamp without time zone"}])
    end

    it 'append data into one table' do
      client.drop_table('public', 'simple_table_foo')
      create_table = <<SQL
  create table simple_table_foo (
    id integer,
    col2 varchar(20),
    PRIMARY KEY(id) );
SQL
      client.execute(create_table)
      data = [
        { :id => 1, :col2 => 'value2a' },
      ]
      input = ETL::Input::Array.new(data)
      simple_orgs_schema = client.table_schema('public', 'simple_table_foo')
      client.append_rows(input, { 'simple_table_foo' => simple_orgs_schema }, nil)
      r = client.fetch('Select * from simple_table_foo order by id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:id=>1, :col2=>"value2a"}])

      data = [
        { :id => 3, :col2 => 'value2b' },
      ]
      input2 = ETL::Input::Array.new(data)

      client.append_rows(input2, { 'simple_table_foo' => simple_orgs_schema }, nil)
      r = client.fetch('Select * from simple_table_foo order by id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:id=>1, :col2=>"value2a"}, {:id=>3, :col2=>"value2b"}])
    end

    it 'upsert data into one table' do
      client.drop_table('public', 'simple_orgs')
      create_table = <<SQL
  create table simple_orgs (
    id integer,
    col2 varchar(20),
    PRIMARY KEY(id) );
SQL
      client.execute(create_table)
      data = [
        { :id => 1, :col2 => 'value2a' },
        { :id => 2, :col2 => 'value2b' },
        { :id => 3, :col2 => 'value2c' },
        { :id => 4, :col2 => "value2c \n aghonce" }, # newline should be removed
      ]
      input = ETL::Input::Array.new(data)
      simple_orgs_schema = client.table_schema('public','simple_orgs_2')
      client.upsert_rows(input, { 'simple_orgs' => simple_orgs_schema }, nil)
      r = client.fetch('Select * from simple_orgs order by id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:id=>1, :col2=>"value2a"}, {:id=>2, :col2=>"value2b"}, {:id=>3, :col2=>"value2c"}, {:id=>4, :col2=>"value2c   aghonce"}])
    end

    it 'upsert data into two tables with splitter' do
      client.drop_table('public', 'simple_orgs_2')
      client.drop_table('public', 'simple_orgs_history')
      create_table = <<SQL
  create table simple_orgs_2 (
    id integer,
    col2 varchar(20),
    PRIMARY KEY(id) );

  create table simple_orgs_history (
    h_id integer,
    id integer,
    PRIMARY KEY(h_id) );
SQL
      client.execute(create_table)
      data = [
        { :h_id => 4, :id => 1, :col2 => 'value2a' },
        { :h_id => 5, :id => 2, :col2 => 'value2b' },
        { :h_id => 6, :id => 3, :col2 => 'value2c' }
      ]
      input = ETL::Input::Array.new(data)
      simple_orgs_schema = client.table_schema('public', 'simple_orgs_2')
      simple_orgs_history_schema = client.table_schema('public', 'simple_orgs_history')
      row_splitter = ::ETL::Transform::SplitRow.SplitByTableSchemas([simple_orgs_schema, simple_orgs_history_schema])
      client.upsert_rows(input, { 'simple_orgs_2' => simple_orgs_schema, 'simple_orgs_history' => simple_orgs_history_schema }, row_splitter)
      r = client.fetch('Select * from simple_orgs_2 order by id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:id=>1, :col2=>"value2a"}, {:id=>2, :col2=>"value2b"}, {:id=>3, :col2=>"value2c"}])

      r = client.fetch('Select * from simple_orgs_history order by h_id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:h_id=>4, :id=>1}, {:h_id=>5, :id=>2}, {:h_id=>6, :id=>3}])
    end

    it 'move data by unloading and copying' do
      target_table = 'test_target_table_1'
      client.drop_table('public', table_name)
      sql = "create table #{table_name} (day datetime NOT NULL, attribute varchar(100), PRIMARY KEY (day));"
      client.execute(sql)

      insert_sql = <<SQL
    insert into #{table_name} values
      ('2015-04-01', 'rain'),
      ('2015-04-02', 'snow'),
      ('2015-04-03', 'sun')
SQL
      client.execute(insert_sql)

      client.drop_table('public', target_table)
      sql = "create table #{target_table} (day datetime NOT NULL, attribute varchar(100), PRIMARY KEY (day));"
      client.execute(sql)

      client.unload_to_s3("select * from #{table_name}", s3_destination)
      client.copy_from_s3(target_table, s3_destination, [])
      expect(client.count_row_by_s3(s3_destination)).to eq(3)

      sql = "select count(*) from #{target_table}"
      r = client.fetch(sql)
      values = []
      r.map { |v| values << v }
      expect(values).to eq([{:count=>3}])

      # Delete s3 files
      client.delete_object_from_s3(bucket, table_name, table_name)
    end

    context 'validate copy from s3' do
      def create_table(c)
        c.drop_table('public', 'test_s3_copy')
        create_table = <<SQL
    create table test_s3_copy (
      id integer,
      col2 varchar(5),
      PRIMARY KEY(id) );
SQL
        c.execute(create_table)
      end

      it 'Upload succeeds first time' do
        create_table(client)

        csv_file_path = "valid_csv_#{SecureRandom.hex(5)}"
        csv_file = ::CSV.open(csv_file_path, 'w', col_sep: client.delimiter)
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '2']))
        csv_file.close

        error = client.copy_from_s3_with_retries("test_s3_copy", csv_file_path, [])
        sleep(3)

        expect(error[0]).to eq(nil)
        result = client.fetch("select count(*) from test_s3_copy").all

        expect(result[0][:count]).to eq(1)
      end

      it "Second row fails, is removed then data uploaded" do
        create_table(client)
        client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
        client2.stl_load_retries = 4

        csv_file_name = "valid_csv_#{SecureRandom.hex(5)}"
        csv_file = ::CSV.open(csv_file_name, 'w', col_sep: client.delimiter)
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '1']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '3']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '38hn8v3089j3v'])) # string too long
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '5']))
        csv_file.close

        error = client2.copy_from_s3_with_retries("test_s3_copy", csv_file_name, [])
        expect(error[0]).to include("test_s3_copy_errors")
        expect(error[1]).to include("s3://ss-uw1-stg.redshift-testing/error_lines/")

        result = client2.fetch("select count(*) from test_s3_copy").all
        expect(result[0][:count]).to eq(4)
        ::File.delete(error[0])
      end

      it "Fails when limit of number of retries occurs, get specific error" do
        create_table(client)
        client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
        client2.stl_load_retries = 1

        csv_file_name = "valid_csv_#{SecureRandom.hex(5)}"
        csv_file = ::CSV.open(csv_file_name, 'w', col_sep: client.delimiter)
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, 'jfhcrhnvc89n23irnm9gh2vih28vhbn2v882hv8hbvh8w3n8d'])) # string too long
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '2bflababahabavb']))
        csv_file.close

        found_error = nil
        begin
          client2.copy_from_s3_with_retries("test_s3_copy", csv_file_name, [])
        rescue => e
          found_error = e
        end
        expect(found_error.message).to eq("STL Load error: Reason: 'String length exceeds DDL length', LineNumber: 1, Position: 2, Rawline '1.2bflababahabavb', \nparsed row: '{\"id\"=>\"1\"}'")
        ::File.delete(e.local_error_file)
      end

      it '#copy_multiple_files_from_s3_with_retries' do
        create_table(client)

        csv_file_path = "valid_csv_#{SecureRandom.hex(5)}"
        csv_file = ::CSV.open(csv_file_path, 'w', col_sep: client.delimiter)
        csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '1']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [2, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [3, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [4, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [5, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [6, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [7, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [8, '2']))
        csv_file.add_row(CSV::Row.new(["id", "col2"], [9, '2']))
        csv_file.close

        client.copy_multiple_files_from_s3_with_retries('test_s3_copy', csv_file_path, [])

        result = client.fetch("select count(*) from test_s3_copy").all
        expect(result[0][:count]).to eq(9)
      end
    end

    it "remove line at" do
      csv_file_name = "remove_csv_#{SecureRandom.hex(5)}"
      csv_file = ::CSV.open(csv_file_name, 'w', col_sep: client.delimiter)
      csv_file.add_row(CSV::Row.new(["id", "col2"], [1, 'jfhcrhnvc89n23irnm9gh2vih28vhbn2v882hv8hbvh8w3n8d'])) # string too long
      csv_file.add_row(CSV::Row.new(["id", "col2"], [1, '2bflababahabavb']))
      csv_file.close
      line_removed = ::ETL::Redshift::Client.remove_line_at(2, csv_file_name, "#{csv_file_name}_1")
      expect(line_removed).to eq("1\u00012bflababahabavb\n")
      lines = []
      File.open("#{csv_file_name}_1", "r") do |f|
        f.each_line do |line|
          lines << line
        end
      end
      expect(lines).to eq(["1\u0001jfhcrhnvc89n23irnm9gh2vih28vhbn2v882hv8hbvh8w3n8d\n"])
      ::File.delete(csv_file_name)
      ::File.delete("#{csv_file_name}_1")
    end

    it "#upload_multiple_files_to_s3" do
      create_test_file
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
      create_test_table(client2)
      client2.delimiter = '|'
      client2.upload_multiple_files_to_s3("client_test.csv")
      client2.copy_from_s3('client_test', "ss-uw1-stg.redshift-testing/client_test")

      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(20)
      delete_test_file
    end
  end
end
