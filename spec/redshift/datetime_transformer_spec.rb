require 'etl/core'
require 'etl/redshift/datetime_transformer'
require 'etl/redshift/table'

RSpec.describe "datetime_transformer" do

  it "transforms" do
    table = ETL::Redshift::Table.new(:test_table)
    table.timestamp(:col1)
    table.date(:col2)
    t = ETL::Redshift::DateTimeTransformer.new({"t" => table})
    row = {col1: Time.new(2002, 10, 31), col2: Date.new(2003, 10, 15) }
    t_row = t.transform(row)
    expect(t_row[:col1]).to eq('2002-10-31 00:00:00')
    expect(t_row[:col2]).to eq('2003-10-15')
  end
end
