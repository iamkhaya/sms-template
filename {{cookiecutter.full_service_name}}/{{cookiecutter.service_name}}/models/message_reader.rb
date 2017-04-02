class MessageReader

  def initialize(msg=nil)
    @msg = msg
  end

  def read
    puts "found SQS message : #{@msg}"
    json_msg = JSON.parse(@msg)
  end
end
