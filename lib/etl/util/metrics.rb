module ETL
  # For reporting job & data metrics
  # TODO:
  # * report batch size or row count
  class Metrics
    def initialize(params = {})
      @series = params.fetch(:series)
      @file = params[:file].tap do |f|
        f ? File.new(f, 'w+') : STDOUT
      end
    end

    def point(values, tags: {}, time: Time.now, type: :gauge)
      p = {
        series: @series,
        time: time,
        values: values,
        tags: tags,
        type: type
      }
      publish(p)
    end

    def time(tags: {}, &block)
      start_time = Time.now
      yield tags
      end_time = Time.now
      point({ duration: end_time - start_time }, tags: tags, time: end_time, type: :timer)
    end

    protected

    def publish(point)
      @file.puts(p)
    end
  end
end
