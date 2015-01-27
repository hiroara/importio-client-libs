#
# import.io client library - client classes
#
# This file contains the main classes required to connect to and query import.io APIs
#
# Dependencies: Ruby 1.9, http-cookie
#
# @author: dev@import.io
# @source: https://github.com/import-io/importio-client-libs/tree/master/python
#

require 'importio/errors'
require 'importio/query'
require 'importio/session'

class Importio
  DEFAILT_HOST = 'https://query.import.io'
  # The main import.io client, used for managing the message channel and sending queries and receiving data

  def initialize(user_id=nil, api_key=nil, host=DEFAILT_HOST)
    # Initialises the client library with its configuration
    @host = host
    @proxy_host = nil
    @proxy_port = nil
    @user_id = user_id
    @api_key = api_key
    @username = nil
    @password = nil
    @login_host = nil
    @session = nil
    @queue = Queue.new
  end

  # We use this only for a specific test case
  attr_reader :session

  def proxy(host, port)
    # If you want to configure an HTTP proxy, use this method to do so
    @proxy_host = host
    @proxy_port = port
  end

  def login(username, password, host="https://api.import.io")
    # If you want to use cookie-based authentication, this method will log you in with a username and password to get a session
    @username = username
    @password = password
    @login_host = host

    # If we don't have a session, then connect one
    if @session == nil
      connect()
    end

    # Once connected, do the login
    @session.login(@username, @password, @login_host)
  end

  def reconnect
    # Reconnects the client to the platform by establishing a new session

    # Disconnect an old session, if there is one
    if @session != nil
      disconnect()
    end

    if @username != nil
      login(@username, @password, @login_host)
    else
      connect()
    end
  end

  def connect
    # Connect this client to the import.io server if not already connected

    # Check if there is a session already first
    if @session != nil
      return
    end

    @session = Session::new(self, @host, @user_id, @api_key, @proxy_host, @proxy_port)
    @session.connect()

    # This should be a @queue.clone, but this errors in 2.1 branch of Ruby: #9718
    # q = @queue.clone
    q = Queue.new
    until @queue.empty?
      q.push(@queue.pop(true))
    end
    @queue = Queue.new

    until q.empty?
      query_data = q.pop(true) rescue nil
      if query_data
        query(query_data.query, query_data.callback)
      end
    end
  end

  def disconnect
    # Call this method to ask the client library to disconnect from the import.io server
    # It is best practice to disconnect when you are finished with querying, so as to clean
    # up resources on both the client and server

    if @session != nil
      @session.disconnect()
      @session = nil
    end
  end

  def stop
    # This method stops all of the threads that are currently running in the session
    if @session != nil
      return @session.stop()
    end
  end

  def join
    # This method joins the threads that are running together in the session, so we can wait for them to be finished
    if @session != nil
      return @session.join()
    end
  end

  def query(query, &block)
    # This method takes an import.io Query object and either queues it, or issues it to the server
    # depending on whether the session is connected

    if @session == nil || !@session.connected?
      @queue << { 'query' => query, 'callback' => block }
      return
    end

    @session.query query, &block
  end

  def connected?
    @session != nil && @session.connected?
  end
end
