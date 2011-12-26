
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
  class TransactionAborted < StandardError; end
  class NonTransactionalMethod < StandardError; end
  
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
    
    def to_str
      @name
    end
    
    alias_method :to_s, :to_str
    
    def method_missing *args, &block
      @name.__send__ *args, &block
    end
  end
  
  def key name
    RedisCall::Key.new name
  end
  
  
  class Connection
    def initialize(host, port)
      @connection = Hiredis::Connection.new
      @connection.connect(host, port)
      
      @multi_depth = 0
    end
    
    def connected?
      @connection.connected?
    end
    
    def disconnect
      @connection.disconnect
    end
    
    def inside_transaction?
      @multi_depth != 0
    end
    

    def call *args
               @connection.write(args)
      result = @connection.read
      
      @call_index += 1 if @call_index

      raise result if result.is_a?(Exception)
      
      result
      
    rescue RuntimeError => exception
      if exception.message == "not connected"
        raise(IOError, "Not connected")
      else
        raise(exception)
      end
    end
    
    alias_method :method_missing, :call 
    

    def queued result, &block
      if @queued_handlers
        (@queued_handlers[@call_index] ||= []).push(block)
      else
        yield(result)
      end
    end
    
    def exec
      if (@multi_depth -= 1) == 0
        begin
          unless result = call(:EXEC)
            raise RedisCall::TransactionAborted
          end
          
          drop_results = []

          @queued_handlers.each do |index, handlers|
            result[index] = handlers.inject(result[index]) do |data, handler|
              if handler
                handler.call(data)
              else
                drop_results.push index
                data
              end
            end
          end
          
          drop_results.each {|index| result.delete_at index }
          
          (result.length == 1) ? result.first : result
          
        ensure
          @call_index = @queued_handlers = nil
        end
      end
    end
    
    
    def discard
      if (@multi_depth -= 1) == 0
        begin
          call(:DISCARD) if @connection.connected?
        ensure
          @call_index = @queued_handlers = nil
        end
      end
    end
    
    
    def multi
      call(:MULTI) if (@multi_depth += 1) == 1
      
      @call_index = -1
      @queued_handlers = {}
      
      if block_given?
        begin
          yield
        rescue ScriptError, StandardError => exception
          begin
            discard
          rescue ScriptError, StandardError => discard_exception
            # It is not important to report this error
            discard_exception.report! if discard_exception.respond_to? :report!
          ensure
            raise exception
          end
        end
        exec
      end
    end

  end
  
  
  def self.query(*args, &block)
    self.new(*args).instance_exec(&block)
  end
  
  
  @@config = {}
  
  def self.config= conf
    @@config = conf
  end
  
  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 6379

  def initialize(args = {})
    @host = args[:host] || @@config[:host] || DEFAULT_HOST
    @port = args[:port] || @@config[:port] || DEFAULT_PORT
    
    if args[:connect]
      @connection = Connection.new(@host, @port)
    else
      @pool_key = "redis_#{@host}:#{@port}".to_sym
    end
  end
  
  def connection
    @connection || (Thread.current[@pool_key] ||= Connection.new(@host, @port))
  end
  
  alias_method :connect, :connection
  
  def disconnect(thread = nil, limit = 10)
    begin
      connection.disconnect
    rescue RuntimeError => exception
      raise(exception) if exception.message != "not connected"
    end
      
    Thread.current[@pool_key] = nil if @pool_key
    
    if thread
      begin
        thread.run
      rescue ThreadError => exception
        raise exception if exception.message != "killed thread"
      end
      thread.join(limit)
    end
  end

  def method_missing *args, &block
    connection.__send__ *args, &block
  end

  def insist(retries = 42, *exceptions)
    exceptions.push RedisCall::TransactionAborted
    yield
  rescue *exceptions => exception
    if (retries -= 1) > 0
      retry
    else
      raise exception
    end
  end
  
  
  def rpushex key, ttl, value
    multi do
      queued(rpush key, value) {|result| result}
      queued(expire key, ttl)
    end
  end
  
  def decrzerodelex key, ttl
    multi do
      queued(decr key) do |result|
        del(key) if result <= 0
        result
      end
      queued(expire key, ttl)
    end
  end
  
  def llen key
    queued(call :LLEN, key) {|result| result.to_i}
  end
  
  def getnnil key
    queued(get key) do |result|
      raise(RedisCall::UnexpectedResult, "Key #{key.inspect} expected to be not nil") if result == nil
      result
    end
  end
  
  def getnnili key
    queued(getnnil key) {|result| result.to_i}
  end
  
  def geti key
    queued(get key) {|result| result.to_i}
  end
  
  def lgetall key
    lrange key,  0, -1
  end
  
  def hgetallarr key
    queued(hgetall key) do |raw|
      result = []
      Hash[*raw].each {|k, v| result[k.to_i] = v}
      result
    end
  end
  
  
  module JSON
    def encode element
      Yajl::Encoder.encode(element)
    end
    
    def decode raw
      (result = Yajl::Parser.new.parse(raw)).is_a?(Hash) ? result.with_indifferent_access : result
    end
    
    alias_method :encode_json, :encode
    alias_method :decode_json, :decode
  end

  module KeepSerializedElement
    def encode element
      if element.is_a?(Hash)
        element = element.dup
        element.delete :serialized
      end
      super(element)
    end
    
    def decode raw
      result = super(raw)
      result[:serialized] = raw if result.is_a?(Hash)
      result
    end
  end

end

