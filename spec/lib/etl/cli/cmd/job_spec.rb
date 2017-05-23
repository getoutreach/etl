require 'spec_helper'
require 'etl/cli/cmd/job'
require 'json'

# Matcher for checking batch equivalence
RSpec::Matchers.define :batch_equivalent_to do |expected|
  match { |actual| actual.to_json == expected.to_json }
end

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
            .exactly(2).times
            .with(anything, an_instance_of(ETL::Batch))
          subject.execute
        end
      end
    end
  end
end
