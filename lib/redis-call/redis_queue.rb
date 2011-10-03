
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


class RedisQueue < RedisCall
  
  def self.list
    connect do
      Hash[keys("queue.*").collect {|queue| [key(queue.gsub(/\Aqueue./, '')), llen(queue)]}]
    end
  end
  
  def self.delete *queues
    connect do
      del *(queues.map { |queue| key(:queue)/queue })
    end
  end
  
  
  attr_reader :queue
  
  def initialize(queue = nil, args = {})
    super(args)
    
    @prefix = key :queue
    @queue = queue
  end
  
  def queue_key queue = nil
    @prefix / (queue || @queue)
  end
  
  
  # Returns the number of elements inside the queue after the push operation.
  def push element, queue = nil
    lpush(queue_key(queue), encode(element))
  end
  
  def error_push element, queue = nil
    lpush(queue_key(queue)/:error, encode(element))
  end

  # Returns element
  def pop queue = nil
    decode(rpop(queue_key(queue)))
  end

  def backup_pop_all queue = nil
    result = []
      while raw_element = rpoplpush(queue_key(queue), queue_key(queue)/:backup)
        result.push [decode(raw_element), raw_element]
      end
    result.reverse
  end
  
  # NOTE: It executed concurrently, elements from active queue (not backup) are distributed between requests
  def backup_pop_all_and_backup_elements queue = nil
    backup = backup_elements(queue)
    backup_pop_all(queue) + backup
  end
  
  # Returns element
  def blocking_pop queue = nil
    decode(brpop(queue_key(queue), 0).last)
  end
  
  # Returns element, raw_element
  def backup_blocking_pop queue = nil
    element = decode(raw_element = brpoplpush(queue_key(queue), queue_key(queue)/:backup, 0))
    
    if block_given?
      yield(element)
      remove_backup raw_element, queue
    else
      return element, raw_element
    end
  end
  
  def remove_backup raw_element, queue = nil
    if lrem(queue_key(queue)/:backup, -1, raw_element) != 1
      raise "Not found raw_element #{raw_element.inspect} in queue #{queue_key(queue)/:backup}"
    end
  end
  

  def restore_backup queue = nil
    while element = rpop(queue_key(queue)/:backup)
      if restored = restore_backup_element(element, queue)
        lpush(queue_key(queue), restored)
      end
    end
  end
  
  def restore_backup_element element, queue
    element
  end
  
  module BackupLimit
    BACKUP_LIMIT = 3
    BACKUP_COUNT_KEY = :redis_queue_backup_retry_count

    def restore_backup_element element, queue
      result = decode_json(element)
      
      if result.is_a?(Hash)
        result[BACKUP_COUNT_KEY] ||= 0
        result[BACKUP_COUNT_KEY] += 1
        
        if result[BACKUP_COUNT_KEY] > BACKUP_LIMIT
          error_push encode_json(result), queue
          return nil
        else
          return encode_json(result)
        end
        
      else
        element
      end
    end
  end

  
  def redirect to_queue, queue = nil
#    http://code.google.com/p/redis/issues/detail?id=593
#    brpoplpush(queue_key(queue), queue_key(to_queue), 0)
  end
  
  def elements queue = nil
    lgetall(queue_key(queue)).map {|raw_element| decode(raw_element)}
  end

  def backup_elements queue = nil
    lgetall(queue_key(queue)/:backup).map {|raw_element| [decode(raw_element), raw_element]}
  end

  def encode element
    element
  end
  
  def decode raw
    raw
  end

end

