require 'aws-sdk'
require 'etl'
require 'securerandom'

module ETL
  module S3
    # CSVFilesUploadingStreamer will push csv files into s3
    # when the sum of the files is greater than a specified amount.
    class CSVFilesUploadingStreamer
      attr_accessor :s3_folder, :csv_file_paths
      def initialize(bucket_manager, parts, num_part_shards, opts = {})
        @bucket_manager = bucket_manager
        @parts = parts
        @num_shards = num_part_shards
        @s3_folder = opts.fetch(:s3_folder, SecureRandom.hex(8))
        @delimiter = opts.fetch(:delimiter, "\u0001")
        @max_sum_file_size_mb = opts.fetch(:max_sum_file_size_mb, 100)
        @tmp_dir = opts.fetch(:tmp_dir, '/tmp')

        @row_number = 0
        init_csvs
      end

      def add_row(part, csv_row)
        if @csv_file_paths.count.zero?
          raise StandardError, 'Once last push was called add_row cannot be invoked.'
        end

        add_row_internal(part, csv_row)

        push_data_if_size_exceeded
      end

      def push_last
        # upload files
        @bucket_manager.push(@s3_folder, @csv_file_paths)

        # close/delete local files
        close_delete_csv_files

        # set things to empty
        @csv_files = {}
        @csv_file_paths = {}
        @current_part_shard_index = {}
      end

      private

      def add_row_internal(part, csv_row)
        shard_num = @current_part_shard_index[part]
        key = "#{part}.#{shard_num}"
        value = @csv_files[key]
        value.add_row(csv_row)
        @current_part_shard_index[part] = (shard_num + 1) % @num_shards
        @row_number += 1
      end

      def push_data_if_size_exceeded
        # Doing a size check every time on all the files seems kind of heavy
        # so only doing this ever x number of rows
        should_check_file_size = @row_number % 1000
        return unless should_check_file_size
        return unless sum_file_sizes > @max_sum_file_size_mb

        # upload files
        @bucket_manager.push(@s3_folder, @csv_file_paths)

        # close/delete files
        close_delete_csv_files

        # generate new csv files
        init_csvs
      end

      def init_csvs
        @csv_files = {}
        @csv_file_paths = {}
        @current_part_shard_index = {}
        @parts.each do |p|
          init_part_csvs(p)
        end
      end

      def init_part_csvs(part)
        raise ArgumentError, 'part cannot be nil' if part.nil?
        @current_part_shard_index[part] = 0
        (0..@num_shards).each do |i|
          key = "#{part}.#{i}"
          @csv_file_paths[key] = temp_file(key)
          @csv_files[key] = ::CSV.open(
            @csv_file_paths[key], 'w', col_sep: @delimiter
          )
        end
      end

      def temp_file(part_name)
        # creating a daily file path so if the disk gets full its
        # easy to kill all the days except the current.
        date_path = DateTime.now.strftime('%Y_%m_%d')
        dir_path = "#{@tmp_dir}/s3/#{date_path}"
        FileUtils.makedirs(dir_path) unless Dir.exist?(dir_path)
        tmp_file = "#{dir_path}/#{part_name}_#{SecureRandom.hex(10)}"
        FileUtils.touch(tmp_file)
        tmp_file
      end

      def close_delete_csv_files
        @csv_files.each_pair do |_k, f|
          f.close
        end
        @csv_file_paths.each do |f|
          ::File.delete(f[1])
        end
      end

      def sum_file_sizes
        size = 0
        @csv_file_paths.each_value do |k|
          size += File.size(k)
        end
        megabytes = bytes_to_megabytes(size)
        megabytes
      end

      def bytes_to_megabytes(bytes)
        bytes / (1024.0 * 1024.0)
      end
    end
  end
end
