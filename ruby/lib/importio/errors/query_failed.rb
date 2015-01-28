class Importio::Errors::QueryFailed < StandardError
  attr_reader :code

  def initialize error_message, query=nil, response=nil
    super error_message
    @message = error_message
    @query = query
    @response = response
  end

  def to_s
    return @message if !@response && !@response.data['errorType']
    "#{@message} (#{@response.data['errorType']}, code: #{@response.data['status']})"
  end
end
