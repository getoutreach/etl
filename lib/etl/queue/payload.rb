require 'json'
require 'etl/util/hash_util'
require 'securerandom'

module ETL::Queue

  # Class representing the payload we are putting into and off the job queue
  class Payload
    attr_reader :job_id, :batch_hash, :uuid
    
    def initialize(job_id, batch, uuid = SecureRandom.uuid)
      @job_id = job_id
      @batch_hash = batch.to_h
      @uuid = uuid
    end

    def to_s
      "Payload<uuid=#{@uuid}, job_id=#{@job_id}, batch=#{@batch_hash.to_s}>"
    end
    
    # encodes the payload into a string for storage in a queue
    def encode
      h = {
        :batch => @batch_hash,
        :job_id => @job_id,
        :uuid => @uuid
      }
      h.to_json.to_s
    end
    
    # creates a new Payload object from a string that's been encoded (e.g.
    # one that's been popped off a queue)
    def self.decode(str)
      h = ETL::HashUtil.symbolize_keys(JSON.parse(str))
      ETL::Queue::Payload.new(h[:job_id], h[:batch], h[:uuid])
    end
  end
end
