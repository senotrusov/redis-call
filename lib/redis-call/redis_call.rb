
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
    
    alias_method "/", "+"
    
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
  
  
  def self.connect(*args, &block)
    conn = self.new(*args)
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
  

  def call *args
    @conn.write(args)
    result = @conn.read
    raise result if result.is_a?(Exception)
    result
  rescue RuntimeError => exception
    if exception.message == "not connected"
      raise(IOError, "Not connected")
    else
      raise(exception)
    end
  end
  
  def method_missing *args
    call *args
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
  
  def rpushex key, ttl, value
    multi do
      rpush key, value
      expire key, ttl
    end
  end
  
  def decrzerodel key
    if value = decr(key) <= 0
      del(key) # If exception somehow happens here the key will stay in storage
    end
    value
  end
  
  def llen key
    call(:LLEN, key).to_i
  end
  
  def getnnil key
    value = get key
    raise(RedisCall::UnexpectedResult, "Key #{key.inspect} expected to be not nil") if value == nil
    value
  end
  
  def getnnili key
    getnnil(key).to_i
  end
  
  def geti key
    get(key).to_i
  end
  
  def lgetall key
    lrange key,  0, -1
  end
  
end

