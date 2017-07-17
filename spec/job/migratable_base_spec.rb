require 'etl/job/exec'
require 'etl/job/migratable_base'

class Job < ETL::Job::MigratableBase
  register_job
  
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
      def p
        puts "test output"
      end
    end

    def self.execute
      Put.instance.p
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
  let(:migration_dir) { "#{Dir.pwd}/db" }
  let(:migration_config) {
    { migration_version: 0,
      migration_dir: migration_dir }
  }
  
  context "migration" do
    before { allow(job).to receive(:migration_config).and_return(migration_config) }
    
    it { expect(job.id).to eq("job") }
    it { expect( job.migration_files.length ).to eq(1) }
    it "#migrate" do
      expect { job.migrate }.to output("test output\n").to_stdout
    end 
  
    it "creates run models" do
      jr = ETL::Model::JobRun.create_for_job(job, batch)
      expect(jr.job_id).to eq('job')
      expect(jr.status).to eq("new")
      expect(jr.started_at).to be_nil
      expect(jr.batch).to eq(batch.to_json)
    end

    it "successful run" do
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