
#  Copyright 2011 Stanislav Senotrusov <stan@senotrusov.com>
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


class RedisCall
  
  class UnexpectedResult < StandardError; end
  

  class Key
    def initialize name
      @name = name.to_s
    end
    
    def + name
      self.class.new(@name + '.' + name.to_s)
    end
    
    def inspect
      @name.inspect
    end
    
    def to_s
      @name
    end
  end
  
  def key name
    RedisCall::Key.new name
  end
  

  def self.connect(&block)
    conn = self.new
    result = conn.instance_exec(&block)
    conn.disconnect
    result
  end
  
  DEFAULT_SOCKET = "/tmp/redis.sock"
  DEFAULT_ADDRESS = "127.0.0.1"
  DEFAULT_PORT = "6379"

  def initialize(args = {})
    @conn = Hiredis::Connection.new
    
    if args[:socket]
      @conn.connect_unix(args[:socket])

    elsif args[:address] || args[:port]
      @conn.connect(args[:address] || DEFAULT_ADDRESS, args[:port] || DEFAULT_PORT)

    elsif File.socket?(DEFAULT_SOCKET)
      @conn.connect_unix(DEFAULT_SOCKET)

    else
      @conn.connect(DEFAULT_ADDRESS, DEFAULT_PORT)
    end
    
    
    init(args) if respond_to? :init
  end
  
  def call *args
    @conn.write(args)
    @conn.read
  rescue RuntimeError => exception
    (exception.message == "not connected") ? raise(IOError, "Not connected") : raise(exception)
  end
  
  def method_missing *args
    call *args
  end
  
  
  # The following assert_* function names and the whole concept it's not very thoughtful, but let's see how it goes 
  def assert_ok *args
    if (result = call(*args)) == "OK"
      result
    else
      raise(Hiredis::UnexpectedResult, "Call #{args.inspect}: received #{result.inspect}, expected 'OK'")
    end 
  end
  
  def assert_notnil *args
    if (result = call(*args)) != nil
      result
    else
      raise(Hiredis::UnexpectedResult, "Call #{args.inspect}: received #{result.inspect}, expected not nil")
    end 
  end
  
  def multi(watch = [])
    call :MULTI
    call(:WATCH, *watch) unless watch.empty?
    yield
    call :EXEC
    
  rescue ScriptError, StandardError => exception
    begin
      call :DISCARD
    rescue ScriptError, StandardError => discard_exception
      discard_exception.report! if discard_exception.respond_to? :report!
    ensure
      raise exception
    end
  end
  
  def disconnect(thread = nil, limit = 10)
    begin
      @conn.disconnect
    rescue RuntimeError => exception
      raise(exception) if exception.message != "not connected"
    end
    
    if thread
      begin
        thread.run
      rescue ThreadError => exception
        raise exception if exception.message != "killed thread"
      end
      thread.join(limit)
    end
  end
  
end

