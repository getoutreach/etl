require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/core'

def create_test_file(filename = "client_test_file", rows = 20, duplicate_rows_col1 = 0, duplicate_rows_col2 = 0, delimiter = "|")
  File.open(filename, "w+") do |f|
    rows.times do |i|
      f.write("abc#{i}#{delimiter}def#{i}#{delimiter}ghi#{i}#{delimiter}#{i}\n")
    end
    duplicate_rows_col1.times do |i|
      f.write("abcd#{delimiter}efg#{i}#{delimiter}hij#{i}#{delimiter}#{i + duplicate_rows_col1}\n")
    end
    duplicate_rows_col2.times do |i|
      f.write("abcd#{delimiter}efgh#{delimiter}ijk#{i}#{delimiter}#{i + duplicate_rows_col1 + duplicate_rows_col2}\n")
    end
  end
end

def create_test_data(rows = 20, duplicate_rows_col1 = 0, duplicate_rows_col2 = 0, duplicate_rows_col3 = 0)
  data = []
  rows.times do |i|
    data << { col1: "abc#{i}",
              col2: "def#{i}",
              col3: "ghi#{i}",
              created_at: i }
  end
  duplicate_rows_col1.times do |i|
    data << { col1: "abcd",
              col2: "efg#{i}",
              col3: "hij#{i}",
              created_at: i + duplicate_rows_col1 }
  end
  duplicate_rows_col2.times do |i|
    data << { col1: "abc#{i}",
              col2: "defg",
              col3: "hij#{i}",
              created_at: i + duplicate_rows_col1 + duplicate_rows_col2 }
  end
  duplicate_rows_col3.times do |i|
    data << { col1: "abc#{i}",
              col2: "def#{i}",
              col3: "ghij",
              created_at: i + duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3 }
  end
  data
end

def create_test_data_with_updated_at(rows = 20, duplicate_rows_col1 = 0, duplicate_rows_col2 = 0, duplicate_rows_col3 = 0)
  data = []
  rows.times do |i|
    data << { col1: "abc#{i}",
              col2: "def#{i}",
              col3: "ghi#{i}",
              created_at: i,
              updated_at: 2 * (duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3) + 40 }
  end
  duplicate_rows_col1.times do |i|
    data << { col1: "abcd",
              col2: "efg#{i}",
              col3: "hij#{i}",
              created_at: i + duplicate_rows_col1,
              updated_at: 2 * (duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3) + 30 }
  end
  duplicate_rows_col2.times do |i|
    data << { col1: "abc#{i}",
              col2: "defg",
              col3: "hij#{i}",
              created_at: i + duplicate_rows_col1 + duplicate_rows_col2,
              updated_at: 2 * (duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3) + 20 }
  end
  duplicate_rows_col3.times do |i|
    data << { col1: "abc#{i}",
              col2: "def#{i}",
              col3: "ghij",
              created_at: i + duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3,
              updated_at: 2 * (duplicate_rows_col1 + duplicate_rows_col2 + duplicate_rows_col3) + 10 }
  end
  data
end

def delete_test_file(filename = "client_test_file")
  system( "rm #{filename} ")
end

def create_test_table(client, name = 'client_test', pkeys = nil, with_updated_at = false)
 client.drop_table('public', name)
 primary_keys = ''
 unless pkeys.nil?
   primary_keys = ", PRIMARY KEY(#{pkeys.join(', ')})"
 end
 create_table = <<SQL
    create table #{name} (
      col1 varchar(8),
      col2 varchar(8),
      col3 varchar(8),
      created_at INT
      #{', updated_at INT' if with_updated_at}
      #{primary_keys} );
SQL
  client.execute(create_table)
end

def fill_test_table(client, name = 'client_test', with_updated_at = false)
  sql = <<SQL
  insert into #{name} values
  ('zyx1', 'wvu1', 'tsr1', 0#{', 0' if with_updated_at}),
  ('zyx2', 'wvu2', 'tsr2', 0#{', 0' if with_updated_at}),
  ('zyx3', 'wvu3', 'tsr3', 0#{', 0' if with_updated_at}),
  ('zyx4', 'wvu4', 'tsr4', 0#{', 0' if with_updated_at}),
  ('zyx5', 'wvu5', 'tsr5', 0#{', 0' if with_updated_at}),
  ('abc2', 'def1', 'ghi1', 0#{', 0' if with_updated_at})
SQL
  client.execute(sql)
end

def count_files_with_prefix(client, prefix)
  i = 0
  client.s3_resource.bucket(client.bucket).objects({prefix: prefix}).each do
    i += 1
  end
  i
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
        { :h_id => 6, :id => 3, :col2 => 'value2c' },
        { :h_id => 7, :id => 4, :col2 => nil }
      ]
      input = ETL::Input::Array.new(data)
      simple_orgs_schema = client.table_schema('public', 'simple_orgs_2')
      simple_orgs_history_schema = client.table_schema('public', 'simple_orgs_history')
      row_splitter = ::ETL::Transform::SplitRow.SplitByTableSchemas([simple_orgs_schema, simple_orgs_history_schema])
      client.upsert_rows(input, { 'simple_orgs_2' => simple_orgs_schema, 'simple_orgs_history' => simple_orgs_history_schema }, row_splitter)
      r = client.fetch('Select * from simple_orgs_2 order by id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:id=>1, :col2=>"value2a"}, {:id=>2, :col2=>"value2b"}, {:id=>3, :col2=>"value2c"}, {:id=>4, :col2 => nil }])

      r = client.fetch('Select * from simple_orgs_history order by h_id')
      values = []
      r.map{ |v| values << v }
      expect(values).to eq([{:h_id=>4, :id=>1}, {:h_id=>5, :id=>2}, {:h_id=>6, :id=>3}, {:h_id => 7, :id => 4}])
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
      client.copy_from_s3(target_table, s3_destination, nil)
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

      it '#copy_multiple_files_from_s3' do
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

        client.copy_multiple_files_from_s3('test_s3_copy', csv_file_path, [])

        result = client.fetch("select count(*) from test_s3_copy").all
        expect(result[0][:count]).to eq(9)

        ::File.delete(csv_file_path)
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
      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_file"}).batch_delete!
      expect(client2.s3_resource.bucket(client2.bucket).object('client_test_file/client_test_file_1.csv').exists?).to eq(false)
      client2.upload_multiple_files_to_s3("client_test_file")
      expect(client2.s3_resource.bucket(client2.bucket).object('client_test_file/client_test_file_1.csv').exists?).to eq(true)
      client2.copy_from_s3('client_test', "ss-uw1-stg.redshift-testing/client_test_file/client_test_file")
      client2.remove_chunked_files("client_test_file")

      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(20)
      delete_test_file
      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_file/"}).batch_delete!
      expect(client2.s3_resource.bucket(client2.bucket).object('client_test_file/client_test_file_1.csv').exists?).to eq(false)
    end

    it 'removes folder if it already exists' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
      # make sure the destination folder doesn't exist
      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_file"}).batch_delete!

      # upload a file with 20 rows
      # this gets chunked into 5 files with 4 rows each
      create_test_file("client_test_file", 20)
      client2.upload_multiple_files_to_s3("client_test_file")
      delete_test_file
      client2.remove_chunked_files("client_test_file")

      # now upload another file with only 10 rows
      # this gets chunked into 5 files with 2 rows each
      create_test_file("client_test_file", 10)
      client2.upload_multiple_files_to_s3("client_test_file")
      delete_test_file
      create_test_table(client2)
      client2.delimiter = '|'
      client2.copy_from_s3('client_test', "ss-uw1-stg.redshift-testing/client_test_file/client_test_file")
      client2.remove_chunked_files("client_test_file")

      # clear out the temp files from s3
      expect(count_files_with_prefix(client2, "client_test_file")).to be > 0
      expect(client2.s3_resource.bucket(client2.bucket).object('client_test_file/client_test_file_1.csv').exists?).to eq(true)
      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_file/"}).batch_delete!
      expect(count_files_with_prefix(client2, "client_test_file")).to eq(0)
      expect(client2.s3_resource.bucket(client2.bucket).object('client_test_file/client_test_file_1.csv').exists?).to eq(false)

      # verify we only got 10 rows
      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(10)
    end

    it 'de-duplicates on merge with single column primary key, no updated_at' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])

      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_"}).batch_delete!

      data = create_test_data(10, 10, 10, 10)

      input = ETL::Input::Array.new(data)
      create_test_table(client2, 'client_test', ['col1'])
      table_schema = client2.table_schema('public', 'client_test')
      table_schemas_lookup = { 'client_test' => table_schema }
      fill_test_table(client2, 'client_test')

      client2.upsert_rows(input, table_schemas_lookup, nil, nil, [])

      # verify we didn't leave any files behind
      expect(count_files_with_prefix(client2, "client_test_")).to eq(0)

      # verify we only got 11 rows
      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(16)

      # verify data
      result = client2.fetch("select * from client_test order by col1 asc, col2 asc, col3 asc").all

      expect(result).to eq(
        [
          { :col1=>"abc0", :col2=>"def0", :col3=>"ghij", :created_at=>30 },
          { :col1=>"abc1", :col2=>"def1", :col3=>"ghij", :created_at=>31 },
          { :col1=>"abc2", :col2=>"def2", :col3=>"ghij", :created_at=>32 },
          { :col1=>"abc3", :col2=>"def3", :col3=>"ghij", :created_at=>33 },
          { :col1=>"abc4", :col2=>"def4", :col3=>"ghij", :created_at=>34 },
          { :col1=>"abc5", :col2=>"def5", :col3=>"ghij", :created_at=>35 },
          { :col1=>"abc6", :col2=>"def6", :col3=>"ghij", :created_at=>36 },
          { :col1=>"abc7", :col2=>"def7", :col3=>"ghij", :created_at=>37 },
          { :col1=>"abc8", :col2=>"def8", :col3=>"ghij", :created_at=>38 },
          { :col1=>"abc9", :col2=>"def9", :col3=>"ghij", :created_at=>39 },
          { :col1=>"abcd", :col2=>"efg9", :col3=>"hij9", :created_at=>19 },
          { :col1=>"zyx1", :col2=>"wvu1", :col3=>"tsr1", :created_at=>0 },
          { :col1=>"zyx2", :col2=>"wvu2", :col3=>"tsr2", :created_at=>0 },
          { :col1=>"zyx3", :col2=>"wvu3", :col3=>"tsr3", :created_at=>0 },
          { :col1=>"zyx4", :col2=>"wvu4", :col3=>"tsr4", :created_at=>0 },
          { :col1=>"zyx5", :col2=>"wvu5", :col3=>"tsr5", :created_at=>0 }
        ]
      )
    end

    it 'de-duplicates on merge with single column primary key, with updated_at' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])

      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_"}).batch_delete!

      data = create_test_data_with_updated_at(10, 10, 10, 10)

      input = ETL::Input::Array.new(data)
      create_test_table(client2, 'client_test', ['col1'], true)
      table_schema = client2.table_schema('public', 'client_test')
      table_schemas_lookup = { 'client_test' => table_schema }
      fill_test_table(client2, 'client_test', true)

      client2.upsert_rows(input, table_schemas_lookup, nil, nil, [])

      # verify we didn't leave any files behind
      expect(count_files_with_prefix(client2, "client_test_")).to eq(0)

      # verify we only got 11 rows
      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(16)

      # verify data
      result = client2.fetch("select * from client_test order by col1 asc, col2 asc, col3 asc").all

      expect(result).to eq(
        [
          {:col1=>"abc0", :col2=>"def0", :col3=>"ghi0", :created_at=>0, :updated_at=>100},
          {:col1=>"abc1", :col2=>"def1", :col3=>"ghi1", :created_at=>1, :updated_at=>100},
          {:col1=>"abc2", :col2=>"def2", :col3=>"ghi2", :created_at=>2, :updated_at=>100},
          {:col1=>"abc3", :col2=>"def3", :col3=>"ghi3", :created_at=>3, :updated_at=>100},
          {:col1=>"abc4", :col2=>"def4", :col3=>"ghi4", :created_at=>4, :updated_at=>100},
          {:col1=>"abc5", :col2=>"def5", :col3=>"ghi5", :created_at=>5, :updated_at=>100},
          {:col1=>"abc6", :col2=>"def6", :col3=>"ghi6", :created_at=>6, :updated_at=>100},
          {:col1=>"abc7", :col2=>"def7", :col3=>"ghi7", :created_at=>7, :updated_at=>100},
          {:col1=>"abc8", :col2=>"def8", :col3=>"ghi8", :created_at=>8, :updated_at=>100},
          {:col1=>"abc9", :col2=>"def9", :col3=>"ghi9", :created_at=>9, :updated_at=>100},
          {:col1=>"abcd", :col2=>"efg9", :col3=>"hij9", :created_at=>19, :updated_at=>90},
          {:col1=>"zyx1", :col2=>"wvu1", :col3=>"tsr1", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx2", :col2=>"wvu2", :col3=>"tsr2", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx3", :col2=>"wvu3", :col3=>"tsr3", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx4", :col2=>"wvu4", :col3=>"tsr4", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx5", :col2=>"wvu5", :col3=>"tsr5", :created_at=>0, :updated_at=>0}
        ]
      )
    end

    it 'de-duplicates on merge with multi column primary key, no updated_at ' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])

      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_"}).batch_delete!

      data = create_test_data(10, 10, 10, 10)

      input = ETL::Input::Array.new(data)
      create_test_table(client2, 'client_test', ['col1', 'col2'])
      table_schema = client2.table_schema('public', 'client_test')
      table_schemas_lookup = { 'client_test' => table_schema }
      fill_test_table(client2, 'client_test')

      client2.upsert_rows(input, table_schemas_lookup, nil, nil, [])

      # verify we didn't leave any files behind
      expect(count_files_with_prefix(client2, "client_test_")).to eq(0)

      # verify we only got 11 rows
      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(36)

      # verify data
      result = client2.fetch("select * from client_test order by col1 asc, col2 asc, col3 asc").all

      expect(result).to eq(
        [
          { :col1=>"abc0", :col2=>"def0", :col3=>"ghij", :created_at=>30 },
          { :col1=>"abc0", :col2=>"defg", :col3=>"hij0", :created_at=>20 },
          { :col1=>"abc1", :col2=>"def1", :col3=>"ghij", :created_at=>31 },
          { :col1=>"abc1", :col2=>"defg", :col3=>"hij1", :created_at=>21 },
          { :col1=>"abc2", :col2=>"def1", :col3=>"ghi1", :created_at=>0 },
          { :col1=>"abc2", :col2=>"def2", :col3=>"ghij", :created_at=>32 },
          { :col1=>"abc2", :col2=>"defg", :col3=>"hij2", :created_at=>22 },
          { :col1=>"abc3", :col2=>"def3", :col3=>"ghij", :created_at=>33 },
          { :col1=>"abc3", :col2=>"defg", :col3=>"hij3", :created_at=>23 },
          { :col1=>"abc4", :col2=>"def4", :col3=>"ghij", :created_at=>34 },
          { :col1=>"abc4", :col2=>"defg", :col3=>"hij4", :created_at=>24 },
          { :col1=>"abc5", :col2=>"def5", :col3=>"ghij", :created_at=>35 },
          { :col1=>"abc5", :col2=>"defg", :col3=>"hij5", :created_at=>25 },
          { :col1=>"abc6", :col2=>"def6", :col3=>"ghij", :created_at=>36 },
          { :col1=>"abc6", :col2=>"defg", :col3=>"hij6", :created_at=>26 },
          { :col1=>"abc7", :col2=>"def7", :col3=>"ghij", :created_at=>37 },
          { :col1=>"abc7", :col2=>"defg", :col3=>"hij7", :created_at=>27 },
          { :col1=>"abc8", :col2=>"def8", :col3=>"ghij", :created_at=>38 },
          { :col1=>"abc8", :col2=>"defg", :col3=>"hij8", :created_at=>28 },
          { :col1=>"abc9", :col2=>"def9", :col3=>"ghij", :created_at=>39 },
          { :col1=>"abc9", :col2=>"defg", :col3=>"hij9", :created_at=>29 },
          { :col1=>"abcd", :col2=>"efg0", :col3=>"hij0", :created_at=>10 },
          { :col1=>"abcd", :col2=>"efg1", :col3=>"hij1", :created_at=>11 },
          { :col1=>"abcd", :col2=>"efg2", :col3=>"hij2", :created_at=>12 },
          { :col1=>"abcd", :col2=>"efg3", :col3=>"hij3", :created_at=>13 },
          { :col1=>"abcd", :col2=>"efg4", :col3=>"hij4", :created_at=>14 },
          { :col1=>"abcd", :col2=>"efg5", :col3=>"hij5", :created_at=>15 },
          { :col1=>"abcd", :col2=>"efg6", :col3=>"hij6", :created_at=>16 },
          { :col1=>"abcd", :col2=>"efg7", :col3=>"hij7", :created_at=>17 },
          { :col1=>"abcd", :col2=>"efg8", :col3=>"hij8", :created_at=>18 },
          { :col1=>"abcd", :col2=>"efg9", :col3=>"hij9", :created_at=>19 },
          { :col1=>"zyx1", :col2=>"wvu1", :col3=>"tsr1", :created_at=>0 },
          { :col1=>"zyx2", :col2=>"wvu2", :col3=>"tsr2", :created_at=>0 },
          { :col1=>"zyx3", :col2=>"wvu3", :col3=>"tsr3", :created_at=>0 },
          { :col1=>"zyx4", :col2=>"wvu4", :col3=>"tsr4", :created_at=>0 },
          { :col1=>"zyx5", :col2=>"wvu5", :col3=>"tsr5", :created_at=>0 }
        ]
      )
    end

    it 'de-duplicates on merge with multi column primary key, with updated_at' do
      client2 = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])

      client2.s3_resource.bucket(client2.bucket).objects({prefix: "client_test_"}).batch_delete!

      data = create_test_data_with_updated_at(10, 10, 10, 10)

      input = ETL::Input::Array.new(data)
      create_test_table(client2, 'client_test', ['col1', 'col2'], true)
      table_schema = client2.table_schema('public', 'client_test')
      table_schemas_lookup = { 'client_test' => table_schema }
      fill_test_table(client2, 'client_test', true)

      client2.upsert_rows(input, table_schemas_lookup, nil, nil, [])

      # verify we didn't leave any files behind
      expect(count_files_with_prefix(client2, "client_test_")).to eq(0)

      # verify we only got 11 rows
      result = client2.fetch("select count(*) from client_test").all
      expect(result[0][:count]).to eq(36)

      # verify data
      result = client2.fetch("select * from client_test order by col1 asc, col2 asc, col3 asc").all

      expect(result).to eq(
        [
          {:col1=>"abc0", :col2=>"def0", :col3=>"ghi0", :created_at=>0, :updated_at=>100},
          {:col1=>"abc0", :col2=>"defg", :col3=>"hij0", :created_at=>20, :updated_at=>80},
          {:col1=>"abc1", :col2=>"def1", :col3=>"ghi1", :created_at=>1, :updated_at=>100},
          {:col1=>"abc1", :col2=>"defg", :col3=>"hij1", :created_at=>21, :updated_at=>80},
          {:col1=>"abc2", :col2=>"def1", :col3=>"ghi1", :created_at=>0, :updated_at=>0},
          {:col1=>"abc2", :col2=>"def2", :col3=>"ghi2", :created_at=>2, :updated_at=>100},
          {:col1=>"abc2", :col2=>"defg", :col3=>"hij2", :created_at=>22, :updated_at=>80},
          {:col1=>"abc3", :col2=>"def3", :col3=>"ghi3", :created_at=>3, :updated_at=>100},
          {:col1=>"abc3", :col2=>"defg", :col3=>"hij3", :created_at=>23, :updated_at=>80},
          {:col1=>"abc4", :col2=>"def4", :col3=>"ghi4", :created_at=>4, :updated_at=>100},
          {:col1=>"abc4", :col2=>"defg", :col3=>"hij4", :created_at=>24, :updated_at=>80},
          {:col1=>"abc5", :col2=>"def5", :col3=>"ghi5", :created_at=>5, :updated_at=>100},
          {:col1=>"abc5", :col2=>"defg", :col3=>"hij5", :created_at=>25, :updated_at=>80},
          {:col1=>"abc6", :col2=>"def6", :col3=>"ghi6", :created_at=>6, :updated_at=>100},
          {:col1=>"abc6", :col2=>"defg", :col3=>"hij6", :created_at=>26, :updated_at=>80},
          {:col1=>"abc7", :col2=>"def7", :col3=>"ghi7", :created_at=>7, :updated_at=>100},
          {:col1=>"abc7", :col2=>"defg", :col3=>"hij7", :created_at=>27, :updated_at=>80},
          {:col1=>"abc8", :col2=>"def8", :col3=>"ghi8", :created_at=>8, :updated_at=>100},
          {:col1=>"abc8", :col2=>"defg", :col3=>"hij8", :created_at=>28, :updated_at=>80},
          {:col1=>"abc9", :col2=>"def9", :col3=>"ghi9", :created_at=>9, :updated_at=>100},
          {:col1=>"abc9", :col2=>"defg", :col3=>"hij9", :created_at=>29, :updated_at=>80},
          {:col1=>"abcd", :col2=>"efg0", :col3=>"hij0", :created_at=>10, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg1", :col3=>"hij1", :created_at=>11, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg2", :col3=>"hij2", :created_at=>12, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg3", :col3=>"hij3", :created_at=>13, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg4", :col3=>"hij4", :created_at=>14, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg5", :col3=>"hij5", :created_at=>15, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg6", :col3=>"hij6", :created_at=>16, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg7", :col3=>"hij7", :created_at=>17, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg8", :col3=>"hij8", :created_at=>18, :updated_at=>90},
          {:col1=>"abcd", :col2=>"efg9", :col3=>"hij9", :created_at=>19, :updated_at=>90},
          {:col1=>"zyx1", :col2=>"wvu1", :col3=>"tsr1", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx2", :col2=>"wvu2", :col3=>"tsr2", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx3", :col2=>"wvu3", :col3=>"tsr3", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx4", :col2=>"wvu4", :col3=>"tsr4", :created_at=>0, :updated_at=>0},
          {:col1=>"zyx5", :col2=>"wvu5", :col3=>"tsr5", :created_at=>0, :updated_at=>0}
        ]
      )
    end
  end
end
