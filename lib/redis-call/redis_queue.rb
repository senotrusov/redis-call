
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


module RedisQueue
  class BackupElementNotFound < StandardError; end
    
  @config = {}
  
  def self.config= conf
    @config = conf
  end
  
  def self.config
    @config
  end
  
  class Simple < RedisCall
    class << self 
      alias_method :find, :new
    
      def all
        (query{keys("queue.*")}.collect {|name| name.gsub(/\Aqueue\./, '')} | RedisQueue.config.keys).sort.collect {|name| new(name)}
      end
      
      def delete *names
        query do
          del *(names.map{|name| key(:queue)/name})
        end
      end
    end
    
    attr_reader :name, :config
    
    def initialize(name = nil, args = {})
      super(args)
      
      @name = key(name)
      @key = key(:queue)/name
      @config = RedisQueue.config[name] || {}
    end
    
    def encode element
      element
    end
    
    def decode element
      element
    end
    
    
    # Returns the number of elements inside the queue after the push operation.
    def push element
      lpush(@key, encode(element))
    end
    
    def error_push element
      lpush(@key/:error, encode(element))
    end

    def error_push_raw element
      lpush(@key/:error, element)
    end
    
    
    # Returns element
    def pop
      if element = rpop(@key)
        decode(element)
      end
    end

    # Returns element
    def blocking_pop timeout = 0
      if result = brpop(@key, timeout)
        decode(result.last)
      end
    end
    
    def backed_up_pop
      if element = rpoplpush(@key, @key/:backup)
        decode(element)
      end
    end
    
    # Returns element
    def backed_up_blocking_pop timeout = 0
      if raw_element = brpoplpush(@key, @key/:backup, timeout)
        element = decode(raw_element)
        
        if block_given?
          yield(element)
          remove_raw_backup_element raw_element
        else
          return element
        end
      end
    end
    
    def remove_raw_backup_element element
      if lrem(@key/:backup, -1, element) != 1
        raise(RedisQueue::BackupElementNotFound, "Not found element #{element.inspect} in backup queue #{@key/:backup}")
      end
    end
    
    
    def backed_up_pop_all
      result = []
      # We does not call backed_up_pop here, because of the edge case, when element is a string "null" which JSON-decoded as nil
      while element = rpoplpush(@key, @key/:backup)
        result.push decode(element)
      end
      result.reverse
    end
    
    # NOTE: If executed concurrently, elements from active queue (not backup) are distributed between requests
    def backed_up_pop_all_and_backup_elements
      backup = backup_elements
      backed_up_pop_all + backup
    end
    

    def elements
      lgetall(@key).map {|element| decode(element)}
    end

    def backup_elements
      lgetall(@key/:backup).map {|element| decode(element)}
    end
    

    # NOTE: http://code.google.com/p/redis/issues/detail?id=593
    # TODO: to_queue may be kind_if? Key, Queue, String
#    def blocking_redirect to_queue
#      brpoplpush(@key, to_queue, 0)
#    end

    
    def restore_backup
      while element = rpop(@key/:backup)
        if element = filter_backup_element(element)
          lpush(@key, element)
        end
      end
    end
    
    def filter_backup_element element
      element
    end
    
    def length
      llen(@key)
    end
    
    def delete
      del(@key)
    end
    
    alias_method :destroy, :delete
  end
  

  module RestoreBackupLimit
    BACKUP_LIMIT = 3
    BACKUP_COUNT_KEY = :redis_queue_backup_retry_count

    def filter_backup_element element
      result = decode_json(element)
      
      if result.is_a?(Hash)
        result[BACKUP_COUNT_KEY] ||= 0
        result[BACKUP_COUNT_KEY] += 1
        
        if result[BACKUP_COUNT_KEY] > BACKUP_LIMIT
          error_push_raw encode_json(result)
          return nil
        else
          return encode_json(result)
        end
        
      else
        element
      end
    end
  end

  class Base < Simple
    include RedisCall::JSON
    include RedisCall::KeepSerializedElement
    
    include RedisQueue::RestoreBackupLimit
    
    extend ActiveModel::Naming
    include ActiveModel::Conversion

    def persisted?
      true
    end
    
    def id
      @name
    end
    
    def action name
      @config[:actions].find {|item| item[:action] = name.to_s}
    end
  end

end

