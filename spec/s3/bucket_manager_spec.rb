require 'etl/s3/bucket_manager'
require 'etl/core'
require 'securerandom'

RSpec.describe 's3' do
  context 'bucket_manager tests' do
    it 'push files to bucket and delete them with 1 thread' do
      test_push(1)
    end

    it 'push files to bucket and delete them with 2 threads' do
      test_push(2)
    end

    def test_push(num_threads)
      s3_paths = []
      bm = ::ETL::S3::BucketManager.new(ETL.config.aws[:etl][:s3_bucket], ETL.config.aws[:etl][:region], num_threads)
      s3_folder = "bucket_manager_test/#{SecureRandom.hex(8)}"
      begin
        csv_file_path1 = "valid_csv_1#{SecureRandom.hex(5)}"
        csv_file_path2 = "valid_csv_2#{SecureRandom.hex(5)}"
        csv_file1 = ::CSV.open(csv_file_path1, 'w', col_sep: '|')
        csv_file2 = ::CSV.open(csv_file_path2, 'w', col_sep: '|')
        csv_file1.add_row(CSV::Row.new(%w[id col2], [1, '1']))
        csv_file2.add_row(CSV::Row.new(%w[id col2], [2, '2']))
        csv_file1.add_row(CSV::Row.new(%w[id col2], [4, '2']))
        csv_file2.add_row(CSV::Row.new(%w[id col2], [5, '2']))
        csv_file1.add_row(CSV::Row.new(%w[id col2], [7, '2']))
        csv_file2.add_row(CSV::Row.new(%w[id col2], [8, '2']))
        csv_file1.close
        csv_file2.close

        s3_paths = bm.push(s3_folder, [csv_file_path1, csv_file_path2]).sort!
        expect(s3_paths).to eq(["#{s3_folder}/#{csv_file_path1}",
                                "#{s3_folder}/#{csv_file_path2}"].sort!)

        keys = bm.object_keys_with_prefix(s3_folder).sort!
        expect(keys).to eq(["#{s3_folder}/#{csv_file_path1}",
                            "#{s3_folder}/#{csv_file_path2}"].sort!)
      ensure
        ::File.delete(csv_file_path1)
        ::File.delete(csv_file_path2)
        bm.delete_objects_with_prefix(s3_folder)
      end
    end
    it 'Initialize with non-existant bucket should error' do
      expect { ::ETL::S3::BucketManager.new('non-existent-bucket-10', 'us-west-2') }.to raise_error(
        ArgumentError,
        "The bucket 'non-existent-bucket-10' doesn't exist"
      )
    end
  end
end
