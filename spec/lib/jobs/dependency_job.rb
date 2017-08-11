module ETL::Test
  class DependencyJob < ETL::Job::Base
    def self.input_class
      Class.new(ETL::Input::Array) do
        def initialize(params = {})
          super( params.merge({
            data: (1..14).to_a.map { |x| { "num_days" => x } }
            } ) )
        end
      end
    end
    
    def self.output_class
      Class.new(ETL::Output::CSV) do
        def initialize(params = {})
          super(params.merge( { load_strategy: :insert_table } ))
          
          define_schema do |t|
            t.date(:date)
            t.int(:num_days)
            t.date(:future_date)
          end
        end
        
        # Adds computed columns
        def transform_row(row)
          row.merge({
            "date" => Time.now,
            "future_date" => Time.now + (row["num_days"] * 24 * 60 * 60),
          })
        end
      end
    end
    
    def batch_factory_class
      ETL::BatchFactory::Hour
    end
  end
end

# A2 and A3 depend on A1
class A1 < ETL::Test::DependencyJob
  register_job
end

class A2 < ETL::Test::DependencyJob
  register_job_with_parent("a1")
end

class A3 < ETL::Test::DependencyJob
  register_job_with_parent("a1")
end

# B1 and B2 are parents og B3
class B1 < ETL::Test::DependencyJob
  register_job
end

class B2 < ETL::Test::DependencyJob
  register_job
end

class B3 < ETL::Test::DependencyJob
  register_job_with_parent("b1")
  register_job_with_parent("b2")
end
