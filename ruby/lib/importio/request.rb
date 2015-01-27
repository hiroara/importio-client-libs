class Importio
  class Request

    # These variables serve to identify this client and its version to the server
    CLIENT_NAME = 'import.io Ruby client'
    CLIENT_VERSION = '2.0.0'

    attr_reader :uri, :http, :request
    def initialize session, url, data, options={}
      @session = session

      @uri = URI.parse url

      @request = Net::HTTP::Post.new uri.request_uri
      @request.body = data

      @http = Net::HTTP.new uri.host, uri.port, @proxy_host, @proxy_port
      @http.use_ssl = uri.scheme == 'https'

      @request.content_type = options[:content_type] if options.key? :content_type
    end

    def send_request
      set_headers!
      # Makes a network request
      @http.request(@request).tap do |response|
        next unless cookies = response.get_fields('set-cookie')
        @session.set_cookies_for @uri, cookies
      end
    end

    def cookie= cookie
      @request['Cookie'] = cookie
    end

    private
    def set_headers!
      @request['import-io-client'] = CLIENT_NAME
      @request['import-io-client-version'] = CLIENT_VERSION
    end
  end
end
