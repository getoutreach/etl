require 'etl/s3/csv_files_uploading_streamer'
require 'etl/core'
require 'securerandom'
module TestCSVHelpers
  class MockBucketManager
    attr_accessor :num_pushes
    def initialize
      @num_pushes = 0
    end

    def push(_s3_folder, _files)
      @num_pushes += 1
    end
  end
end

RSpec.describe 's3' do
  context 'csvfiles_uploading_streamer' do
    let(:parts) { %w[orgs orgs_history] }
    let(:opts) { { s3_folder: 'test', max_sum_file_size_mb: 1 } }
    let(:orgs_column_names) { %w[id col1 col2 col3 col4 col5] }
    let(:org_history_column_names) { %w[ida cola1 cola2 cola3 cola4 cola5] }
    let(:column_names_arr) { [orgs_column_names, org_history_column_names] }
    let(:table_names) { %w[orgs orgs_history] }
    it 'Validate then when size exceeds 5MBs the files are uploaded' do
      test_writing_rows(100, 1)
    end

    it 'Write rows, never go over limit, validate no push' do
      test_writing_rows(50, 0)
    end

    it 'Write rows, validate push end' do
      hash = test_writing_rows(50, 0)
      hash[:streamer].push_last
      expect(hash[:bucket_manager].num_pushes).to eq(1)
    end

    it 'Write rows, validate push end' do
      hash = test_writing_rows(50, 0)
      hash[:streamer].push_last
      expect { hash[:streamer].add_row(orgs_column_names, nil) }.to raise_error(StandardError, 'Once last push was called add_row cannot be invoked.')
    end

    def test_writing_rows(num_rows, expected_pushes)
      mbm = TestCSVHelpers::MockBucketManager.new
      streamer = ::ETL::S3::CSVFilesUploadingStreamer.new(mbm, parts, 3, opts)
      (0..num_rows).each do |i|
        index = i % 2
        values = []
        (0..5).each do |_j|
          values << SecureRandom.hex(1000)
        end

        csv_row = CSV::Row.new(column_names_arr[index], values)
        streamer.add_row(table_names[index], csv_row)
      end
      expect(mbm.num_pushes).to eq(expected_pushes)
      { streamer: streamer, bucket_manager: mbm }
    end
  end
end
