class Importio::Errors::RequestFailed < StandardError
  attr_reader :code

  def initialize error_message, code=nil
    super error_message
    @message = error_message
    @code = code
  end

  def to_s
    return @message unless self.code
    "#{@message} (code: #{self.code})"
  end
end
