require 'spec_helper'
require 'etl/cli/cmd/migration'
require 'json'

RSpec.describe ETL::Cli::Cmd::Migration::Create do
  let(:described_instance) do
    instance = described_class.new('etl migration create', {})
    instance.parse(args)
    instance
  end

  let(:table) { "test_table" }
  let(:outputdir) { "#{Dir.pwd}/db" }
  let(:args) { ["--table", table, "--outputdir", outputdir, "--inputdir", ENV['ETL_CONFIG_DIR']] }

  before(:all) do
    puts "create db"
    Dir.mkdir 'db'
  end

  after(:all) do
    system( "rm -rf #{Dir.pwd}/db")
  end


  context 'with table_name' do
    before { allow(described_instance).to receive(:schema_map).and_return({ :day=>[:timestamp, nil], :attribute=>[:varchar, 100] }) }

    it '#up_sql' do
      #expect( described_instance ).to receive(:schema_map).with({ :day=>[:timestamp, nil], :attribute=>[:varchar, 100] })
      expect( described_instance.up_sql ).to eq( "create table #{table} ( day timestamp, attribute varchar(100) )" )
    end

    it '#execute' do
      described_instance.execute
      expect(File).to exist("#{outputdir}/0001_#{table}.rb") 
    end

    it '#execute' do
      system( "rm #{outputdir}/db/*")
      described_instance.execute
      described_instance.execute
      expect(File).to exist("#{outputdir}/0001_#{table}.rb") 
      expect(File).to exist("#{outputdir}/0002_#{table}.rb") 
    end
  end
end
