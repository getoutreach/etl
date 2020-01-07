require 'etl/queue/payload.rb'
RSpec.describe "payload" do

  let(:id) { 123 }
  let(:batch) { ETL::Batch.new({:foo => "abc", :bar => "xyz"}) }
  let(:uuid) { 'baab7aea-39dd-4fdf-b6cd-6ec33a3dad38' }
  let(:encoded_payload) { '{"batch":{"bar":"xyz","foo":"abc"},"job_id":123,"uuid":"baab7aea-39dd-4fdf-b6cd-6ec33a3dad38"}' }

  context 'with no uuid specified' do
    let(:uuid) { nil }

    it 'creates a uuid' do
      p = ETL::Queue::Payload.new(id, batch)
      expect(p.uuid).not_to be_nil
    end
  end

  describe 'encode' do
    it 'encodes the correct values' do
      p = ETL::Queue::Payload.new(id, batch, uuid)
      enc = p.encode
      expect(enc).to eq(encoded_payload)
    end
  end

  describe 'decode' do
    it 'decodes the correct values' do
      p = ETL::Queue::Payload.decode(encoded_payload)
      expect(p.job_id).to eq(id)
      expect(p.batch_hash).to eq(batch.to_h)
      expect(p.uuid).to eq(uuid)
      expect(p.to_s).to eq('Payload<uuid=baab7aea-39dd-4fdf-b6cd-6ec33a3dad38, job_id=123, batch={:bar=>"xyz", :foo=>"abc"}>')
    end

    context 'with null uuid' do
      let(:encoded_payload) { '{"batch":{"bar":"xyz","foo":"abc"},"job_id":123,"uuid":null}' }

      it 'decodes the correct values' do
        p = ETL::Queue::Payload.decode(encoded_payload)
        expect(p.job_id).to eq(id)
        expect(p.batch_hash).to eq(batch.to_h)
        expect(p.uuid).to be_nil
        expect(p.to_s).to eq('Payload<uuid=, job_id=123, batch={:bar=>"xyz", :foo=>"abc"}>')
      end
    end

    context 'without uuid' do
      let(:encoded_payload) { '{"batch":{"bar":"xyz","foo":"abc"},"job_id":123}' }
      it 'decodes the correct values' do
        p = ETL::Queue::Payload.decode(encoded_payload)
        expect(p.job_id).to eq(id)
        expect(p.batch_hash).to eq(batch.to_h)
        expect(p.uuid).to be_nil
        expect(p.to_s).to eq('Payload<uuid=, job_id=123, batch={:bar=>"xyz", :foo=>"abc"}>')
      end
    end
  end
end
