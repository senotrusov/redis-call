
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
    def self.all
      (query{keys("queue.*")}.collect {|name| name.gsub(/\Aqueue\./, '')} | RedisQueue.config.keys).sort.collect {|name| new(name)}
    end
    
    def self.delete *queues
      query do
        del *(queues.map { |queue| key(:queue)/queue })
      end
    end
    
    
    attr_reader :queue, :config
    
    def initialize(queue = nil, args = {})
      super(args)
      
      @prefix = key :queue
      @queue = queue
      @config = RedisQueue.config[queue]
    end
    
    def queue_key queue = nil
      @prefix / (queue || @queue)
    end
    
    def encode element
      element
    end
    
    def decode element
      element
    end
    
    
    # Returns the number of elements inside the queue after the push operation.
    def push element, queue = nil
      lpush(queue_key(queue), encode(element))
    end
    
    def error_push element, queue = nil
      lpush(queue_key(queue)/:error, encode(element))
    end

    def error_push_raw element, queue = nil
      lpush(queue_key(queue)/:error, element)
    end
    
    
    # Returns element
    def pop queue = nil
      if element = rpop(queue_key(queue))
        decode(element)
      end
    end

    # Returns element
    def blocking_pop timeout = 0, queue = nil
      if result = brpop(queue_key(queue), timeout)
        decode(result.last)
      end
    end
    
    def backed_up_pop queue = nil
      if element = rpoplpush(queue_key(queue), queue_key(queue)/:backup)
        decode(element)
      end
    end
    
    # Returns element
    def backed_up_blocking_pop timeout = 0, queue = nil
      if raw_element = brpoplpush(queue_key(queue), queue_key(queue)/:backup, timeout)
        element = decode(raw_element)
        
        if block_given?
          yield(element)
          remove_raw_backup_element raw_element, queue
        else
          return element
        end
      end
    end
    
    def remove_raw_backup_element element, queue = nil
      if lrem(queue_key(queue)/:backup, -1, element) != 1
        raise(RedisQueue::BackupElementNotFound, "Not found element #{element.inspect} in backup queue #{queue_key(queue)/:backup}")
      end
    end
    
    
    def backed_up_pop_all queue = nil
      result = []
      # We does not call backed_up_pop here, because of the edge case, when element is a string "null" which JSON-decoded as nil
      while element = rpoplpush(queue_key(queue), queue_key(queue)/:backup)
        result.push decode(element)
      end
      result.reverse
    end
    
    # NOTE: If executed concurrently, elements from active queue (not backup) are distributed between requests
    def backed_up_pop_all_and_backup_elements queue = nil
      backup = backup_elements(queue)
      backed_up_pop_all(queue) + backup
    end
    

    def elements queue = nil
      lgetall(queue_key(queue)).map {|element| decode(element)}
    end

    def backup_elements queue = nil
      lgetall(queue_key(queue)/:backup).map {|element| decode(element)}
    end
    

    # http://code.google.com/p/redis/issues/detail?id=593
    def blocking_redirect to_queue, queue = nil
      brpoplpush(queue_key(queue), queue_key(to_queue), 0)
    end

    
    def restore_backup queue = nil
      while element = rpop(queue_key(queue)/:backup)
        if element = filter_backup_element(element, queue)
          lpush(queue_key(queue), element)
        end
      end
    end
    
    def filter_backup_element element, queue
      element
    end
    
    def length queue = nil
      llen(queue_key(queue))
    end
    
    def delete queue = nil
      del(queue_key(queue))
    end
    
    alias_method :destroy, :delete
  end
  

  module RestoreBackupLimit
    BACKUP_LIMIT = 3
    BACKUP_COUNT_KEY = :redis_queue_backup_retry_count

    def filter_backup_element element, queue
      result = decode_json(element)
      
      if result.is_a?(Hash)
        result[BACKUP_COUNT_KEY] ||= 0
        result[BACKUP_COUNT_KEY] += 1
        
        if result[BACKUP_COUNT_KEY] > BACKUP_LIMIT
          error_push_raw encode_json(result), queue
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
      @queue
    end
    
    def action name
      @config[:actions].find {|item| item[:action] = name.to_s}
    end
  end

end

