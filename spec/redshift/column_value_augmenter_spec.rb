require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/redshift/column_value_augmenter'
require 'etl/core'

module Test
  class Augmenter< ::ETL::Transform::Base
    def initialize(augmenter)
      @augmenter = augmenter
      @count = 10
    end
    def transform(row)
      row = @augmenter.transform(row)
      row["dw_id"] = @count.to_s if row["dw_id"].nil?
      @count = @count + 1
      row
    end
  end
end

RSpec.describe "redshift column_augmenter" do
  context "Test redshift column_augmenter" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "test_table_2" }
    it "augment a row with a surrogate key and old values" do
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name)
      table.string(:id)
      table.string(:dw_id)
      table.string(:info)
      table.date(:created_at)
      table.date(:ended_at)
      table.add_primarykey(:dw_id)
      client.create_table(table)
      data = [
        { "id" => "1", "info" => "foo" },
        { "id" => "2", "info" => "other" },
      ]
      input = ETL::Input::Array.new(data)
      table_schema = client.table_schema(table_name)
      # insert test data
      client.execute("INSERT INTO #{table_name} (dw_id, id, info) VALUES ('6', '1', 'bar')")

      column_augmenter = ::ETL::Redshift::ColumnValueAugmenter.new(client, table_schema, ["dw_id"], ["id"], ["info"], nil)
      # Wrapping the column augmenter so that rows that don't have dw_id's set will get values
      augmenter = Test::Augmenter.new(column_augmenter)
      client.upsert_rows(input, {table_schema.name => table_schema}, augmenter)

      r = client.execute("Select * from #{table_name} ORDER BY dw_id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"id"=>"2", "dw_id"=>"11", "info"=>"other", "created_at"=>nil, "ended_at"=>nil},
                            {"id"=>"1", "dw_id"=>"6", "info"=>"foo", "created_at"=>nil, "ended_at"=>nil}])
      client.drop_table(table_name)
    end
  end
end
