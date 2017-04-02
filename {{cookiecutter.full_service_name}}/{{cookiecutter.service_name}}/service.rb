require 'ffi-rzmq'
require 'google/protobuf'
require 'rubygems'

# Load compiled protobuffs

load 'person_pb.rb'

Thread.abort_on_exception = true

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    STDERR.puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| STDERR.puts(callstack) }
    true
  end
end

ctx = ZMQ::Context.create(1)
STDERR.puts "Failed to create a Context" unless ctx

#Lets set ourselves up for replies
Thread.new do
  rep_sock = ctx.socket(ZMQ::REP)
  rc = rep_sock.bind('tcp://127.0.0.1:2200')
  error_check(rc)

  message = ''
  while ZMQ::Util.resultcode_ok?(rc)
    rc = rep_sock.recv_string(message)
    break if error_check(rc)

    decoded = Person.decode(message)
    puts "Received request '#{decoded.name}'"
    # You must send a reply back to the REQ socket.
    # Otherwise the REQ socket will be unable to send any more requests
    rc = rep_sock.send_string('Polo!')
    break if error_check(rc)
  end

  # the while loop ends when the call to ctx.terminate below causes the socket to return
  # an error code

  # always close a socket when we're done with it otherwise
  # the context termination will hang indefinitely
  error_check(rep_sock.close)
  puts "Closed REP socket; terminating thread..."
end
