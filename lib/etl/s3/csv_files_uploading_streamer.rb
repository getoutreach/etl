require 'aws-sdk'
require 'etl'
require 'securerandom'
require_relative '../util/tmp_util'

module ETL
  module S3
    # CSVFilesUploadingStreamer will push csv files into s3
    # when the sum of the files is greater than a specified amount.
    class CSVFilesUploadingStreamer
      attr_accessor :s3_folder, :csv_file_paths, :data_pushed
      def initialize(bucket_manager, parts, num_part_shards, opts = {})
        @bucket_manager = bucket_manager
        @parts = parts
        @num_shards = num_part_shards
        @s3_folder = opts.fetch(:s3_folder, SecureRandom.hex(8))
        @delimiter = opts.fetch(:delimiter, "\u0001")
        @max_sum_file_size_mb = opts.fetch(:max_sum_file_size_mb, 50)
        @tmp_dir = opts.fetch(:tmp_dir, '/tmp')

        @row_number = 0
        @data_pushed = false
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
        # Close files
        close_files

        # upload files
        @bucket_manager.push(@s3_folder, @csv_file_paths.values)
        @data_pushed = true

        # delete local files
        delete_files

        # set things to empty
        @csv_files = {}
        @csv_file_paths = {}
        @current_part_shard_index = {}
      end

      def close_files
        @csv_files.each_pair do |_k, f|
          f.close
        end
      end

      def delete_files
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

        # close the files prior to upload
        close_files

        # upload files
        @bucket_manager.push(@s3_folder, @csv_file_paths.values)
        @data_pushed = true

        # close/delete files
        delete_files

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
          @csv_file_paths[key] = ETL::TmpUtil.by_day(@tmp_dir, 's3', key)
          @csv_files[key] = ::CSV.open(
            @csv_file_paths[key], 'w', col_sep: @delimiter
          )
        end
      end
    end
  end
end
