require 'spec_helper'

describe Importio do
  after { client.disconnect if client.connected? }

  let(:client) { Importio::new userguid, apikey, "https://query.#{host}" }
  let(:host) { 'import.io' }
  let(:apikey) { ENV['APIKEY'] }
  let(:userguid) { ENV['GUID'] }
  let(:username) { ENV['USER'] }
  let(:password) { ENV['PASSWORD'] }
  let(:callback) do
    @queries = []
    @messages = []
    lambda do |query, message|
      @queries << query
      @messages << message
    end
  end

  let(:expected_data) do
    [
      "Iron Man",
      "Captain America",
      "Hulk",
      "Thor",
      "Black Widow",
      "Hawkeye"
    ]
  end

  describe 'specifying incorrect username and password raises an exception' do
    let(:userguid) { nil }
    let(:apikey) { nil }
    subject { client.login SecureRandom.uuid, SecureRandom.uuid, "https://api.#{host}" }
    it { expect{ subject }.to raise_error Importio::Errors::RequestFailed }
  end

  describe 'providing an incorrect user GUID raises an exception' do
    let(:userguid) { SecureRandom.uuid }
    subject { client.connect }
    it { expect{ subject }.to raise_error Importio::Errors::RequestFailed }
  end

  describe 'providing an incorrect API key raises an exception' do
    let(:apikey) { SecureRandom.uuid }
    subject { client.connect }
    it { expect{ subject }.to raise_error Importio::Errors::RequestFailed }
  end

  describe 'querying a source that doesn\'t exist returns an error' do
    before { client.connect }

    subject do
      client.query({"input"=>{"query"=>"server"}, "connectorGuids"=>[SecureRandom.uuid]}, &callback)
      client.join
      client.disconnect
      @messages.find { |message| message['type'] == 'MESSAGE' }
    end

    it do
      expect(subject['data']).to have_key 'errorType'
      expect(subject['data']['errorType']).to eq 'ConnectorNotFoundException'
    end
  end

  describe 'querying a source that doesn\'t exist returns an error' do
    before { client.connect }

    subject do
      client.query({"input"=>{"query"=>"server"},"connectorGuids"=>["eeba9430-bdf2-46c8-9dab-e1ca3c322339"]}, &callback)
      client.join
      client.disconnect
      @messages.find { |message| message['type'] == 'MESSAGE' }
    end

    it do
      expect(subject['data']).to have_key 'errorType'
      expect(subject['data']['errorType']).to eq 'UnauthorizedException'
    end
  end

  describe 'querying a working source with user GUID and API key' do
    before { client.connect }

    subject do
      client.query({"input"=>{"query"=>"server"},"connectorGuids"=>["1ac5de1d-cf28-4e8a-b56f-3c42a24b1ef2"]}, &callback )
      client.join
      client.disconnect
      @messages.select{ |message| message['type'] == 'MESSAGE' }.flat_map do |message|
        message['data']['results'].map { |result| result['name'] }
      end
    end

    it { expect(subject).to match expected_data }
  end

  describe 'querying a working source with username and password' do
    before do
      client.login(username, password, "https://api.#{host}")
      client.connect
    end
    let(:userguid) { nil }
    let(:apikey) { nil }

    subject do
      client.query({"input"=>{"query"=>"server"}, "connectorGuids"=>["1ac5de1d-cf28-4e8a-b56f-3c42a24b1ef2"]}, &callback)
      client.join
      client.disconnect
      @messages.select{ |message| message['type'] == 'MESSAGE' }.flat_map do |message|
        message['data']['results'].map { |result| result['name'] }
      end
    end

    it { expect(subject).to match expected_data }
  end

  describe 'querying a working source twice, with a client ID change in the middle' do
    before { client.connect }

    test8callback = lambda do |query, message|
      if message["type"] == "DISCONNECT"
        test8disconnects = test8disconnects + 1
      end
    end

    subject do
      client.query({"input"=>{"query"=>"server"},"connectorGuids"=>["1ac5de1d-cf28-4e8a-b56f-3c42a24b1ef2"]}, &callback )
      client.join

      client.session.client_id = "random"
      # This query will fail
      client.query({"input"=>{"query"=>"server"},"connectorGuids"=>["1ac5de1d-cf28-4e8a-b56f-3c42a24b1ef2"]}, &callback )
      client.query({"input"=>{"query"=>"server"},"connectorGuids"=>["1ac5de1d-cf28-4e8a-b56f-3c42a24b1ef2"]}, &callback )
      client.join

      client.disconnect

      @messages.select{ |message| message['type'] == 'MESSAGE' }.flat_map do |message|
        message['data']['results'].map { |result| result['name'] }
      end
    end

    it do
      subject.each_slice(expected_data.length) do |names|
        expect(names).to match expected_data
      end
    end
  end
end
