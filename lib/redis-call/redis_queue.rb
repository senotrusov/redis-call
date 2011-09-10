
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
      Hash[keys("queue.*").collect {|queue| [key(queue), llen(queue)]}]
    end
  end
  
  def self.delete *queues
    connect do
      del *queues
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
    lpush(queue_key(queue), element)
  end
  
  def error_push element, queue = nil
    lpush(queue_key(queue)/:error, element)
  end

  # Returns element
  def pop queue = nil
    brpop(queue_key(queue), 0).last
  end
  
  # Returns element
  def backup_pop queue = nil
    element = brpoplpush(queue_key(queue), queue_key(queue)/:backup, 0)
    
    if block_given?
      yield(element)
      remove_backup element, queue
    else
      return element
    end
  end
  
  def remove_backup element, queue = nil
    if lrem(queue_key(queue)/:backup, -1, element) != 1
      raise "Not found element #{element.inspect} in queue #{queue_key(queue)/:backup}"
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
  
  def redirect to_queue, queue = nil
#    http://code.google.com/p/redis/issues/detail?id=593
#    brpoplpush(queue_key(queue), queue_key(to_queue), 0)
  end
end

