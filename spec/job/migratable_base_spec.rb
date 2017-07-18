require 'etl/job/exec'
require 'etl/job/migratable_base'

class Job < ETL::Job::MigratableBase 
  register_job

  def initialize(b)
    super(b)
    @target_version = 1
    @migration_dir = "#{Dir.pwd}/db" 
  end

  def output
    o = super
    o.success = 34
    o.message = 'congrats!'
    o.sleep_time = nil
    o.exception = nil
    o
  end
end

RSpec.describe "migratablejob" do
  
  # Make migration_dir and put files to be executed in migrate
  before(:all) do
    Dir.mkdir 'db'
    f = File.open("#{Dir.pwd}/db/0001_job.rb", "w")
    s = <<-END
  require 'singleton'
  module Migration
    class Put
      include Singleton

      def up
        puts "test output up"
      end

      def down 
        puts "test output down"
      end
    end

    def self.up
      Put.instance.up
    end

    def self.down
      Put.instance.down
    end
  end
END
    f << s
    f.close()
  end

  after(:all) do
    system( "rm -rf #{Dir.pwd}/db")
  end

  before(:each) do
    ETL::Model::JobRun.dataset.delete
  end
  
  let(:batch) { ETL::Batch.new() }
  let(:job_id) { 'job' }
  let(:payload) { ETL::Queue::Payload.new(job_id, batch) }
  let(:job) { Job.new(ETL::Batch.new(payload.batch_hash)) }
  let(:job_exec) { ETL::Job::Exec.new(payload) }
  
  context "migration" do
    it { expect(job.id).to eq("job") }
    it { expect( job.migration_files.length ).to eq(1) }
    it "#migrate up" do
      allow(job).to receive(:deploy_version).and_return(0)
      expect { job.migrate }.to output("test output up\n").to_stdout
    end 

    it "#migrate down" do
      allow(job).to receive(:deploy_version).and_return(2)
      expect { job.migrate }.to output("test output down\n").to_stdout
    end 
  
    it "creates run models" do
      jr = ETL::Model::JobRun.create_for_job(job, batch)
      expect(jr.job_id).to eq('job')
      expect(jr.status).to eq("new")
      expect(jr.started_at).to be_nil
      expect(jr.batch).to eq(batch.to_json)
    end

    it "successful run" do
      ENV["JOB_SCHEMA_VERSION"] = "0"
      jr = job_exec.run

      # check this object
      expect(jr.job_id).to eq(job.id)
      expect(jr.status).to eq("success")
      expect(jr.queued_at).to be_nil
      expect(jr.started_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.strftime('%F')).to eq(DateTime.now.strftime('%F'))
      expect(jr.ended_at.to_i() - jr.started_at.to_i()).to be <= 1
      expect(jr.batch).to eq(batch.to_json)
      expect(jr.rows_processed).to eq(job.output.success)
      expect(jr.message).to eq(job.output.message)
    end
  end
end