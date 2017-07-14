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
  let(:dir) { "#{Dir.pwd}/db" }
  let(:args) { ["--table", table, "--outputdir", dir, "--inputdir", dir] }

  before(:all) do
    Dir.mkdir 'db'
    f = File.open("#{Dir.pwd}/db/migration_config.yml", "w")
    s = <<-END
test_table:
  source_db_params:
    host: "localhost"
    adapter: "mysql2"
    database: "mysql"
    user: "root"
    password: ""
  columns:
    day: day
    attribute: attr
END
    f << s
    f.close()
    system( "cp #{Dir.pwd}/etc/erb_templates/redshift_migration.erb #{Dir.pwd}/db/")
  end

  after(:all) do
    system( "rm -rf #{Dir.pwd}/db")
  end

  context 'with table_name' do
    before { allow(described_instance).to receive(:source_schema).and_return([ { COLUMN_NAME: "day", DATA_TYPE: "timestamp", CHARACTER_MAXIMUM_LENGTH: nil },
                                                                               { COLUMN_NAME: "attribute", DATA_TYPE: "varchar", CHARACTER_MAXIMUM_LENGTH: "100" }]) }

    it '#schema_map' do
      expect( described_instance.schema_map ).to eq({ "day"=>[:timestamp, nil], "attr"=>[:varchar, "100"] })
    end

    it '#up_sql' do
      expect( described_instance.up_sql ).to eq( "create table #{table} ( day timestamp, attr varchar(100) )" )
    end

    it '#execute' do
      described_instance.execute
      expect(File).to exist("#{dir}/0001_#{table}.rb") 
    end

    it '#execute' do
      system( "rm #{dir}/db/*")
      described_instance.execute
      described_instance.execute
      expect(File).to exist("#{dir}/0001_#{table}.rb") 
      expect(File).to exist("#{dir}/0002_#{table}.rb") 
    end
  end
end
