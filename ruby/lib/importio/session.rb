require 'thread'
require 'uri'
require 'net/http'
require 'json'
require 'cgi'
require 'http-cookie'
require 'securerandom'

require 'importio/session/request'

class Importio
  class Session
    # Session manager, used for managing the message channel, sending queries and receiving data

    extend Forwardable
    def_delegators :@io, :logger

    def initialize(io, host=DEFAILT_HOST, user_id=nil, api_key=nil, proxy_host=nil, proxy_port=nil)
      # Initialises the client library with its configuration
      @io = io
      @msg_id = 1
      @client_id = nil
      @url = "#{host}/query/comet/"
      @messaging_channel = '/messaging'
      @queries = Hash.new
      @user_id = user_id
      @api_key = api_key
      @data_queue = Queue.new
      @connected = false
      @connecting = false
      @disconnecting = false
      @polling = false

      @threads = []

      @cookie_jar = HTTP::CookieJar.new

      @proxy_host = proxy_host
      @proxy_port = proxy_port
    end

    # We use this only for a specific test case
    attr_accessor :client_id

    def connected?
      @connected
    end

    def make_request(url, data, options={})
      Request.new self, url, data, options
    end

    def set_cookies_for uri, cookies
      cookies.each { |value| @cookie_jar.parse value, uri }
    end

    def encode(dict)
      # Encodes a dictionary to x-www-form format
      dict.map{|k,v| "#{CGI.escape(k)}=#{CGI.escape(v)}"}.join("&")
    end

    def login(username, password, host="https://api.import.io")
      # If you want to use cookie-based authentication, this method will log you in with a username and password to get a session
      data = encode('username' => username, 'password' => password)
      request = make_request("#{host}/auth/login", data )
      request.send_request.tap do |response|
        raise Importio::Errors::RequestFailed.new('Could not log in', response.code) unless response.code == '200'
      end
    end

    def request(channel, path="", data={}, ignore_failure=false)
      # Helper method that makes a generic request on the messaging channel

      # These are CometD configuration values that are common to all requests we need to send
      data["channel"] = channel
      data["connectionType"] = "long-polling"

      # We need to increment the message ID with each request that we send
      data["id"] = @msg_id
      @msg_id += 1

      # If we have a client ID, then we need to send that (will be provided on handshake)
      data["clientId"] = @client_id unless @client_id == nil

      # Build the URL that we are going to request
      url = "#{@url}#{path}"

      # If the user has chosen API key authentication, we need to send the API key with each request
      if @api_key != nil
        q = encode({ "_user" => @user_id, "_apikey" => @api_key })
        url = "#{url}?#{q}"
      end

      # Build the request object we are going to use to initialise the request
      body = JSON.dump([data])
      request = make_request url, body, content_type: 'application/json;charset=UTF-8'
      request.cookie = HTTP::Cookie.cookie_value @cookie_jar.cookies(@uri)

      # Send the request itself
      response = request.send_request

      # Don't process the response if we've disconnected in the meantime
      return if !@connected and !@connecting

      # If the server responds non-200 we have a serious issue (configuration wrong or server down)
      unless response.code == '200'
        error_message = "Unable to connect to import.io for url #{url}"
        error = Importio::Errors::RequestFailed.new error_message, response.code
        ignore_failure ? self.logger.error(error.message) : raise(error)
      end

      response.body = JSON.parse(response.body)

      # Iterate through each of the messages in the response content
      response.body.each do |msg|
        # If the message is not successful, i.e. an import.io server error has occurred, decide what action to take
        if msg.has_key?("successful") && msg["successful"] != true
          error_message = "Unsuccessful request: #{msg}"
          next if @disconnecting || !@connected || @connecting
          # If we get a 402 unknown client we need to reconnect
          if msg["error"] == "402::Unknown client"
            self.logger.error "402 received, reconnecting"
            @io.reconnect()
          else
            ignore_failure ? self.logger.error(error_message) : raise(Importio::Errors::RequestFailed, error_message)
          end
        end

        # Ignore messages that come back on a CometD channel that we have not subscribed to
        next if msg["channel"] != @messaging_channel

        # Now we have a valid message on the right channel, queue it up to be processed
        @data_queue.push(msg["data"])
      end

      response
    end

    def handshake
      # This method uses the request helper to make a CometD handshake request to register the client on the server
      handshake = request '/meta/handshake', 'handshake',
        'version' => '1.0', 'minimumVersion' => '0.9', 'supportedConnectionTypes' => ['long-polling'],
        'advice' => { 'timeout' => 60000, 'interval' => 0 }

      if handshake == nil
        return
      end

      # Set the Client ID from the handshake's response
      @client_id = handshake.body[0]['clientId']
    end

    def subscribe(channel)
      # This method uses the request helper to issue a CometD subscription request for this client on the server
      request('/meta/subscribe', '', {'subscription'=>channel})
    end

    def connect
      # Connect this client to the import.io server if not already connected
      # Don't connect again if we're already connected
      return if @connected || @connecting

      @connecting = true

      # Do the hanshake request to register the client on the server
      handshake

      # Register this client with a subscription to our chosen message channel
      subscribe @messaging_channel

      # Now we are subscribed, we can set the client as connected
      @connected = true

      clear_threads!
      start_poll!
      start_poll_queue!

      @connecting = false

      return unless block_given?
      yield.tap { self.disconnect }
    end

    def disconnect
      # Call this method to ask the client library to disconnect from the import.io server
      # It is best practice to disconnect when you are finished with querying, so as to clean
      # up resources on both the client and server

      # Maintain a local value of the queries, and then erase them from the class
      q = @queries.clone
      @queries = Hash.new

      # Set the flag to notify handlers that we are disconnecting, i.e. open connect calls will fail
      @disconnecting = true

      # Set the connection status flag in the library to prevent any other requests going out
      @connected = false

      # Make the disconnect request to the server
      request("/meta/disconnect");

      # Now we are disconnected we need to remove the client ID
      @client_id = nil

      # We are done disconnecting so reset the flag
      @disconnecting = false

      # Send a "disconnected" message to all of the current queries, and then remove them
      q.each { |key, query| query._on_message 'type' => 'DISCONNECT', 'requestId' => key }
    end

    def join
      # This method joins the threads that are running together, so we can wait for them to be finished
      sleep 1 while @connected && @queries.length > 0
    end

    def poll_queue
      # This method is called in a new thread to poll the queue of messages returned from the server
      # and process them

      # This while will mean the thread keeps going until the client library is disconnected
      # Attempt to process the last message on the queue
      process_message @data_queue.pop while @connected
    end

    def process_message(data)
      # This method is called by the queue poller to handle messages that are received from the import.io
      # CometD server

      # First we need to look up which query object the message corresponds to, based on its request ID
      request_id = data["requestId"]
      query = @queries[request_id]

      # If we don't recognise the client ID, then do not process the message
      if query == nil
        self.logger.warn "No open query #{query}: #{JSON.pretty_generate(data)}"
        return
      end

      # Call the message callback on the query object with the data
      query._on_message(data)

      # Clean up the query map if the query itself is finished
      @queries.delete request_id if query.finished?
    end

    def query query, &block
      # This method takes an import.io Query object and issues it to the server, calling the callback
      # whenever a relevant message is received

      # Set the request ID to a random GUID
      # This allows us to track which messages correspond to which query
      query['requestId'] = SecureRandom.uuid
      # Construct a new query state tracker and store it in our map of currently running queries
      @queries[query['requestId']] = Query::new query, &block
      # Issue the query to the server
      request '/service/query', '', 'data' => query
    end

    protected
    def poll
      # This method is called in a new thread to open long-polling HTTP connections to the import.io
      # CometD server so that we can wait for any messages that the server needs to send to us

      return if @polling

      @polling = true

      # While loop means we keep making connections until manually disconnected
      # Use the request helper to make the connect call to the CometD endpoint
      request '/meta/connect', 'connect', {}, false while @connected

      @polling = false
    end

    private
    def start_poll!
      # Ruby's HTTP requests are synchronous - so that user apps can run while we are waiting for long connections
      # from the import.io server, we need to pass the long-polling connection off to a thread so it doesn't block
      # anything else
      @threads << Thread.new(self) do |context|
        context.poll rescue Thread.main.raise $!
      end
    end

    def start_poll_queue!
      # Similarly with the polling, we need to handle queued messages in a separate thread too
      @threads << Thread.new(self) do |context|
        context.poll_queue rescue Thread.main.raise $!
      end
    end

    def clear_threads!
      # This method stops all of the threads that are currently running
      @threads.each { |thread| thread.terminate }
      @threads = []
    end
  end
end
