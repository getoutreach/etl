require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/redshift/date_table_id_augmenter'
require 'etl/core'

RSpec.describe "redshift id_augmenter_factory" do
  context "Test redshift augmenter" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "test_table_3" }
    it "augment a row with surrogate key" do
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name)
      table.string(:id)
      table.date(:ct)
      table.int(:ct_dt_id)
      table.add_primarykey(:id)
      client.create_table(table)
      data = [
        { "id" => "1", "ct" => "2015-03-31 01:12:34" },
        { "id" => "2", "ct" => "2020-03-31 01:15:34" },
      ]
      input = ETL::Input::Array.new(data)
      schema = client.table_schema(table_name)
      date_table_augmenter = ::ETL::Redshift::DateTableIDAugmenter.new([schema])
      schema_lookup = {table_name => schema }
      client.upsert_rows(input, schema_lookup, date_table_augmenter)
      r = client.execute("Select * from #{table_name} ORDER BY id")
      expect(r.ntuples).to eq(2)
      expect(r.values).to eq([["1", "2015-03-31", "20150331"], ["2", "2020-03-31", "20200331"]])
      client.drop_table(table_name)
    end
  end
end
