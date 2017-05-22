require 'spec_helper'
require 'etl/cli/cmd/job'

RSpec.describe ETL::Cli::Cmd::Job::Run do
  subject(:described_instance) do
    described_class.new('etl job run', {}).tap do |cmd|
      cmd.parse(args)
    end
  end
  let(:args) { [job_expr] }

  let(:job_expr) { 'days_in_future' }

  describe '#execute' do
    it 'runs jobs' do
      expect(subject).to receive(:run_batch).with(job_expr, an_instance_of(ETL::Batch))
      subject.execute
    end

    context 'with --batch' do
      let(:batch_string) { '{"key": "value"}' }
      let(:args) { args.concat(['--batch', batch_string]) }
      let(:batch) { ETL::BatchFactory.parse!(batch_string) }

      it 'runs jobs' do
        expect(subject).to receive(:run_batch).with(job_expr, batch)
        subject.execute
      end

      context 'and --match' do
        let(:args) { args.concat(['--match']) }
        it 'fails' do
          expect{ subject.execute }.to raise_error(/cannot pass batch/i)
        end
      end
    end

    context 'with --match' do
      let(:args) { args.concat(['--match']) }

      context 'matching one' do
        let(:job_expr) { 'days' }
        it 'runs job' do
          expect(subject).to receive(:run_batch).with(job_expr, an_instance_of(ETL::Batch))
        end
      end
      context 'matching none' do
        let(:job_expr) { 'maze' }
        it 'runs job' do
          expect(subject).not_to receive(:run_batch)
        end
      end
    end
  end
end
