require 'spec_helper'
require 'etl/cli/cmd/job'
require 'json'
require_relative '../../../jobs/days_in_future'
require_relative '../../../jobs/null'

# Matcher for checking batch equivalence
RSpec::Matchers.define :batch_equivalent_to do |expected|
  match { |actual| actual.to_json == expected.to_json }
end

# When run independently, the job manager loads only these jobs; but when run with
# other specs, other classes (like in spec/job_spec.rb) register themselves and change
# the behavior. Stub the registered jobs to avoid that.
REGISTERED_JOBS = {
  'days_in_future' => ETL::Test::DaysInFuture,
  'null' => ETL::Test::Null
}

RSpec.describe ETL::Cli::Cmd::Job::List do
  subject(:described_instance) do
    described_class.new('etl job run', {}).tap do |cmd|
      cmd.parse(args)
    end
  end
  let(:args) { [] }

  before(:each) do
    allow(ETL::Job::Manager.instance).to receive(:job_classes).and_return(REGISTERED_JOBS)
  end

  context 'with no args' do
    it 'lists all' do
      expect(STDOUT).to receive(:puts).with(/ * /).exactly(REGISTERED_JOBS.size).times
      subject.execute
    end
  end

  context 'with a filter' do
    let(:filter) { 'days' }
    let(:args) { ['--match', filter] }
    it 'lists matches' do
      expect(STDOUT).to receive(:puts).with(/ * #{filter}/)
      subject.execute
    end
  end
end

RSpec.describe ETL::Cli::Cmd::Job::List do
  subject(:described_instance) do
    described_class.new('etl job run', {}).tap do |cmd|
      cmd.parse(args)
    end
  end
  let(:args) { [] }

  context 'with dependencies' do
    let(:args) { ['--dependency'] }
    it 'list dependencies' do
      $stdout = StringIO.new
      subject.execute
      arr = $stdout.string.split('\n')
      expect(arr[0]).to start_with(" ***")
      a1 = arr[0].index("a1")
      a2 = arr[0].index("a2")
      a3 = arr[0].index("a3")
      expect(a1).to be < a2 
      expect(a1).to be < a3 

      b1 = arr[0].index("b1")
      b2 = arr[0].index("b2")
      b3 = arr[0].index("b3")
      expect(b1).to be < b3 
      expect(b2).to be < b3 

      c1 = arr[0].index("c1")
      c2 = arr[0].index("c2")
      d1 = arr[0].index("d1")
      cd2 = arr[0].index("c_d2")
      cd3 = arr[0].index("c_d3")
      expect(c1).to be < c2 
      expect(c1).to be < cd2 
      expect(d1).to be < cd2 
      expect(cd2).to be < cd3 
    end
  end

  context 'with dependencies and filter' do
    let(:filter) { 'b' }
    let(:args) { ['--dependency', '--match', filter] }
    it 'list dependencies with filter' do
      $stdout = StringIO.new
      subject.execute
      arr = $stdout.string.split('\n')
      expect(arr[0]).to start_with(" ***")
      a1 = arr[0].index("a1")
      c1 = arr[0].index("c1")
      expect(a1).to be nil 
      expect(c1).to be nil 

      b1 = arr[0].index("b1")
      b2 = arr[0].index("b2")
      b3 = arr[0].index("b3")
      expect(b1).to be < b3 
      expect(b2).to be < b3 
    end
  end
end

RSpec.describe ETL::Cli::Cmd::Job::Run do
  subject(:described_instance) do
    described_class.new('etl job run', {}).tap do |cmd|
      cmd.parse(args)
    end
  end
  let(:args) { [job_expr] }

  before(:each) do
    allow(ETL::Job::Manager.instance).to receive(:job_classes).and_return(REGISTERED_JOBS)
  end

  let(:job_expr) { 'days_in_future' }

  describe '#execute' do
    it 'runs jobs' do
      expect(subject).to receive(:run_batch).with(job_expr, an_instance_of(ETL::Batch))
      subject.execute
    end

    context 'with --batch' do
      let(:batch_string) { '{"key": "value"}' }
      # options have to be before positionals in derpy clamp gem
      # https://github.com/mdub/clamp/issues/39
      let(:args) { ['--batch', batch_string].concat(super()) }
      let(:batch) { ETL::Batch.new(JSON.parse(batch_string)) }

      it 'runs jobs' do
        expect(subject).to receive(:run_batch).with(job_expr, batch_equivalent_to(batch))
        subject.execute
      end

      context 'and --match' do
        let(:args) { ['--match'].concat(super()) }
        it 'fails' do
          expect{ subject.execute }.to raise_error(/cannot pass batch/i)
        end
      end
    end

    context 'with --match' do
      let(:args) { ['--match'].concat(super()) }

      context 'matching one' do
        let(:job_expr) { 'days' }
        it 'runs job' do
          expect(subject).to receive(:run_batch).with(/#{job_expr}/, an_instance_of(ETL::Batch))
          subject.execute
        end
      end

      context 'matching none' do
        let(:job_expr) { 'maze' }
        it 'runs no jobs' do
          expect{ subject.execute }.to raise_error(/no job/i)
        end
      end

      context 'matching all' do
        let(:args) { ['--match'] }
        it 'runs all jobs' do
          expect(subject).to receive(:run_batch)
            .with(an_instance_of(String), an_instance_of(ETL::Batch))
            .exactly(REGISTERED_JOBS.size).times
          subject.execute
        end
      end

      context 'when no jobs are registered' do
        let(:job_expr) { '' }
        before do
          allow(ETL::Job::Manager.instance).to receive(:job_classes).and_return([])
        end
        it 'exits' do
          expect(subject).not_to receive(:run_batch)
          expect{ subject.execute }.to raise_error(SystemExit)
        end
      end
    end
  end
end
