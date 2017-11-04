require 'etl'
require 'etl/cache/base'

RSpec.describe "cache" do
  it "properly finds cached row" do
      data = [
        { "id" => "1", "bento" => "b1", "info" => "foo" },
        { "id" => "1", "bento" => "b2", "info" => "foo1" },
        { "id" => "2", "bento" => "b2", "info" => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id"], false)
      cache.fill(data)
      expect(cache.find_rows({"id" => "1"})).to eq([{"id"=>"1", "bento"=>"b1", "info"=>"foo"}, {"id"=>"1", "bento"=>"b2", "info"=>"foo1"}])
      expect(cache.find_rows({"id" => "2"})).to eq([{"id"=>"2", "bento"=>"b2", "info"=>"other"}])
      expect(cache.find_rows({"id" => "3"})).to eq(nil)
  end
  
  it "properly finds cached row with symbolized rows" do
      data = [
        { :id => "1", :bento => "b1", :info => "foo" },
        { :id => "1", :bento => "b2", :info => "foo1" },
        { :id => "2", :bento => "b2", :info => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id"], true)
      cache.fill(data)
      expect(cache.find_rows({:id => "1"})).to eq([{:id=>"1", :bento=>"b1", :info=>"foo"}, {:id=>"1", :bento =>"b2", :info =>"foo1"}])
      expect(cache.find_rows({:id => "2"})).to eq([{:id=>"2", :bento=>"b2", :info=>"other"}])
      expect(cache.find_rows({:id => "3"})).to eq(nil)
  end
  
  it "properly finds cached row with symbolized rows with multiple keys" do
      data = [
        { :id => "1", :bento => "b1", :info => "foo" },
        { :id => "1", :bento => "b2", :info => "foo1" },
        { :id => "2", :bento => "b2", :info => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id", "bento"], true)
      cache.fill(data)
      expect(cache.find_rows({:id => "1", :bento => "b1"})).to eq([{:id=>"1", :bento=>"b1", :info=>"foo"}])
      expect(cache.find_rows({:id => "2", :bento => "b2"})).to eq([{:id=>"2", :bento=>"b2", :info=>"other"}])
      expect(cache.find_rows({:id => "3", :bento => "b3"})).to eq(nil)
  end

  it "properly finds cached row with non-symbolized rows with multiple keys" do
      data = [
        { "id" => "1", "bento" => "b1", "info" => "foo" },
        { "id" => "1", "bento" => "b2", "info" => "foo1" },
        { "id" => "2", "bento" => "b2", "info" => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id", "bento"], false)
      cache.fill(data)
      expect(cache.find_rows({"id" => "1", "bento" => "b1"})).to eq([{"id"=>"1", "bento"=>"b1", "info"=>"foo"}])
      expect(cache.find_rows({"id" => "2", "bento" => "b2"})).to eq([{"id"=>"2", "bento"=>"b2", "info"=>"other"}])
      expect(cache.find_rows({"id" => "3", "bento" => "b3"})).to eq(nil)
  end
  it "properly finds no rows when no values specified in the key" do
      data = [
        { "id" => "1", "bento" => "b1", "info" => "foo" },
        { "id" => "1", "bento" => "b2", "info" => "foo1" },
        { "id" => "2", "bento" => "b2", "info" => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id", "bento"], false)
      cache.fill(data)
      expect(cache.find_rows({})).to eq(nil)
  end
end
