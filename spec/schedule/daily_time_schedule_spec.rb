require 'etl'
require 'etl/models/job_run_repository'
require 'etl/models/job_run'
require 'etl/schedule/daily_time_schedule'

module Testing
  class ScheduleBaseSpecJob < ETL::Job::Base
    register_job
  end

  # mock class to avoid having to hit the database to test out all the different
  # cases for scheduling
  class JobRunRepositoryMock
    attr_accessor :has_pending, :last_ended_time, :was_successful, :set_has_pending, :set_was_successful

    def has_pending?(_job, _batch)
      @has_pending
    end

    def last_ended(_job, _batch)
      jr = ETL::Model::JobRun.new(nil)
      jr.ended_at = @last_ended_time
      jr
    end
  end

  class TestTimeGenerator
    def initialize(time)
      @time = Time.parse(time)
    end

    def now
      @time
    end
  end
end

RSpec.describe 'schedule/daily_time_schedule' do
  let(:batch) { ETL::Batch.new(day: '2015-03-31') }
  let(:job) { ::Testing::ScheduleBaseSpecJob.new(batch) }

  it 'Test Daily Times Job Ready to run or not with nothing pending' do
    saved_instance = ETL::Model::JobRunRepository.instance
    repo = Testing::JobRunRepositoryMock.new
    ETL::Model::JobRunRepository.instance = repo

    begin
      repo.has_pending = false
      repo.last_ended_time = Time.parse('11:00')

      schedule = ::ETL::Schedule::DailyTimes.new([43_200, 45_000], job, batch)
      schedule.now_generator = Testing::TestTimeGenerator.new('12:00:15')
      expect(schedule.ready?).to eq(true)

      schedule.now_generator = Testing::TestTimeGenerator.new('12:30:05')
      expect(schedule.ready?).to eq(true)

      schedule.now_generator = Testing::TestTimeGenerator.new('12:31:05')
      expect(schedule.ready?).to eq(false)
    ensure
      ETL::Model::JobRunRepository.instance = saved_instance
    end
  end

  it 'Test Daily Times Job Not Ready as job is currently pending/running' do
    saved_instance = ETL::Model::JobRunRepository.instance
    repo = Testing::JobRunRepositoryMock.new
    ETL::Model::JobRunRepository.instance = repo

    begin
      repo.has_pending = true

      schedule = ::ETL::Schedule::DailyTimes.new([1, 30], job, batch)
      schedule.now_generator = Testing::TestTimeGenerator.new('12:01:30 AM')
      expect(schedule.ready?).to eq(false)
    ensure
      ETL::Model::JobRunRepository.instance = saved_instance
    end
  end

  it 'Test DailyTimesByInterval generates right times' do
    schedule = ::ETL::Schedule::DailyTimesByInterval.new('00:00', 360, job, batch)
    expect(schedule.intervals.to_s).to eq('[0, 21600, 43200, 64800, 86400]')
  end
end