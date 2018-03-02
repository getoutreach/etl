require 'aws-sdk'
require 'etl'

module ETL
  module S3
    # BucketManager has has some common operations used used within ETl
    class BucketManager
      attr_accessor :bucket_resource
      def initialize(bucket_name, thread_count = 5)
        @bucket_name = bucket_name
        @thread_count = thread_count
        client = Aws::S3::Client.new
        @bucket_resource = Aws::S3::Bucket.new(
          name: @bucket_name,
          client: client
        )

        raise ArgumentError, "The bucket '#{bucket_name}' doesn't exist, the manager is only used with buckets that have been created" unless @bucket_resource.exists?
      end

      def push(s3_folder, local_filepaths)
        mutex       = Mutex.new
        threads     = []
        file_number = 0

        @bucket_resource.exists?
        s3_files = []
        files = local_filepaths
        @thread_count.times do |i|
          threads[i] = Thread.new do
            until files.empty?
              mutex.synchronize do
                file_number += 1
                Thread.current['filename'] = file_number
              end
              file = begin
                       files.pop
                     rescue => e
                       ETL.logger.debug(
                         "Exception occurred popping file off the stack #{e}"
                       )
                       nil
                     end
              next unless file

              s3_file_name = File.basename(file)
              ETL.logger.debug(
                "[#{Thread.current['file_number']}/#{file}] uploading..."
              )

              s3_obj_path = "#{s3_folder}/#{s3_file_name}"
              @bucket_resource.object(s3_obj_path).upload_file(file)
              s3_files << s3_obj_path
            end
          end
        end
        threads.each(&:join)
        s3_files
      end

      def delete_objects_with_prefix(prefix)
        objects = @bucket_resource.objects(prefix: prefix)
        objects.each { |f| f.delete }
      end
    end
  end
end
