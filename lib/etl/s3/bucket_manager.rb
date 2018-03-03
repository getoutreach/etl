require 'aws-sdk'
require 'etl'
require 'concurrent'

module ETL
  module S3
    # BucketManager has has some common operations used used within ETl
    class BucketManager
      def initialize(bucket_name, region, thread_count = 5)
        @bucket_name = bucket_name
        @thread_count = thread_count
        @client = Aws::S3::Client.new(region: region)
        @s3_resource = Aws::S3::Resource.new(region: region)

        raise ArgumentError, "The bucket '#{@bucket_name}' doesn't exist" unless @s3_resource.bucket(@bucket_name).exists?
      end

      def push(s3_folder, local_filepaths)
        threads = []
        c_arr = Concurrent::Array.new
        local_filepaths.each do |f|
          c_arr << f
        end

        s3_files = []
        files = local_filepaths.clone
        Thread.abort_on_exception = true
        @thread_count.times do |i|
          threads[i] = Thread.new do
            until files.empty?
              file = files.pop
              next unless file
              size = File.size(file)
              next if size.zero?

              s3_file_name = File.basename(file)
              ETL.logger.debug("[#{s3_file_name}] uploading...")
              s3_files << upload_file(file, s3_file_name, s3_folder)
            end
          end
        end
        threads.each(&:join)
        s3_files
      end

      def upload_file(file, s3_file_name, s3_folder)
        s3_obj_path = "#{s3_folder}/#{s3_file_name}"
        @s3_resource.bucket(@bucket_name).object(s3_obj_path).upload_file(file)
        s3_obj_path
      end

      def object_keys_with_prefix(prefix)
        resp = @client.list_objects(bucket: @bucket_name)
        resp[:contents].select { |content| content.key.start_with? prefix }.map(&:key)
      end

      def delete_objects_with_prefix(prefix)
        keys = object_keys_with_prefix(prefix)
        keys.each { |key| @client.delete_object(bucket: @bucket_name, key: key) }
      end
    end
  end
end
