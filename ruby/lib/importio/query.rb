require 'importio/response'

class Importio
  class Query
    # This class represents a single query to the import.io platform

    def initialize(query, &block)
      # Initialises the new query object with inputs and default state
      @query = query
      @jobs_spawned = 0
      @jobs_started = 0
      @jobs_completed = 0
      @_finished = false
      @_callback = block
    end

    def notify data
      callback Response.new(data)
    end

    def finished?
      # Returns boolean - true if the query has been completed or terminated
      @_finished
    end

    private
    def callback response
      # Method that is called when a new message is received
      #
      # Check the type of the message to see what we are working with
      case msg_type = response.type
      when 'SPAWN'
        # A spawn message means that a new job is being initialised on the server
        @jobs_spawned+=1
      when 'INIT', 'START'
        # Init and start indicate that a page of work has been started on the server
        @jobs_started+=1
      when 'STOP'
        # Stop indicates that a job has finished on the server
        @jobs_completed+=1
      end

      # Update the finished state
      # The query is finished if we have started some jobs, we have finished as many as we started, and we have started as many as we have spawned
      # There is a +1 on jobs_spawned because there is an initial spawn to cover initialising all of the jobs for the query
      @_finished = (@jobs_started == @jobs_completed and @jobs_spawned + 1 == @jobs_started and @jobs_started > 0)

      # These error conditions mean the query has been terminated on the server
      # It either errored on the import.io end, the user was not logged in, or the query was cancelled on the server
      @_finished = true if %w[ ERROR UNAUTH CANCEL ].include? msg_type

      # Now we have processed the query state, we can return the data from the message back to listeners
      @_callback.call self, response
    end
  end
end
