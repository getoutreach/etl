require 'mysql2'
require 'etl/core'
require 'etl/migration/sequel'

RSpec.describe "sequel migration from mysql" do
  let(:table_name) { "test_table" }
  let(:migration) { ETL::Migration::Sequel.new(table_name, 0)}
  let(:dbconfig) { { host: "localhost", database: "mysql", username: "root", password: "mysql" } }
  let(:mysql_client) { Mysql2::Client.new(
      :host => dbconfig[:host],
      :database => dbconfig[:database],
      :username => dbconfig[:username],
      :password => dbconfig[:password]) }

  it "fetch mysql schemas" do
    # add data to the test db
    sql = <<SQL
drop table if exists #{table_name};
SQL
    mysql_client.query(sql)

    sql = <<SQL
    create table #{table_name} (day timestamp, attribute varchar(100));
SQL
    mysql_client.query(sql)

    insert_sql = <<SQL
insert into #{table_name} (day, attribute) values
  ('2015-04-01', 'rain'),
  ('2015-04-02', 'snow'),
  ('2015-04-03', 'sun');
SQL

    mysql_client.query(insert_sql)
    mysql_client.store_result while mysql_client.next_result
    expect(migration.schema_map).to eq( {"day"=>"timestamp", "attribute"=>"varchar(100)"} )
  end
end