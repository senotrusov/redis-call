# RedisCall

A [Redis](https://redis.io) access library, which provides:

* Connection management
  * Per-thread connection
  * Shared between threads connection
* Key names construction
* Transactions
  * Queued result handling
  * Retry on optimistic lock fail
* Queues
  * message handling with backup queue
  * restore from backup queue
  * queues in Rails controllers
  * messages as REST resources
  * message handling in separate process
  * graceful stop of blocking operations
* JSON encode/decode
* And nice ruby API

It use [Hiredis](https://github.com/redis/hiredis) to connecto to [Redis](https://redis.io).


## Basic operations
```ruby
r = RedisCall.new

prefix = r.key('foo')

# set the 'foo.bar.qux' to 'content' with 1 day expiration
r.setex prefix+:bar+'qux', 1.day, 'content'
r.setex prefix/:bar/'qux', 1.day, 'content'

# get 'foo.bar.qux'
r.get prefix/:bar/'qux'
```


## Custom storage class

```ruby
class MyStorage < RedisCall
  def store key_name, value
    multi do
      del key(:prefix)/key_name/:some_state
      set key(:prefix)/key_name, value
    end
  end
end
```


# Queues

## Basic operations

```ruby
queue = RedisQueue::Base.new('foo')
queue.push(hello: 'darling')

queue.name
queue.length
queue.backup_length

queue.pop_all
queue.backup_elements
queue.backed_up_pop_all

subqueue = RedisQueue::Base.new(queue.name/:subqueue)
```


## Message handler process

Here is sample standalone message handler process with backup queue.
To handle process-related functionality (start/stop/watchdog), [workety](https://github.com/senotrusov/workety) library is used.

```ruby
class Handler
  def start
    @queue = RedisQueue::Base.new 'queue_name', connect: true
    @queue.restore_backup

    @handler = Thread.networkety do
      loop do
        @queue.backed_up_blocking_pop do |message|
          puts message.inspect
        end
      end
    end
  end

  def join
    @handler.log_join "Handler thread joined"
  end

  def stop
    @queue.disconnect @handler
  end
end
```


## Serialization and custom queue class

Redis operates with string as a queue element. To transfer Ruby classes, serialization technique may be used.
RedisCall use JSON as the transfer format.
In case when you want to transfer not Hash but some custom class (say MyElement),
you may define your own queue class to handle that:

```ruby
class MyQueue < RedisQueue::Base
  def encode element
    super(element.as_json(:except => [:serialized]))
  end

  def decode raw
    MyElement.new(super(raw))
  end
end
```


## Queue metadata

You may store queue metadata in ``config/redis_queue.yml`` file:

```yml
queue_name:
  attr: value
```

and retrieve it:

``` ruby
RedisQueue::Base.new('queue_name').config[:attr]
# => 'value'
```


## Queues in Rails controllers

### A list of queues

```ruby
class QueueListController
  # Queue names currently are not checked for funny characters by library,
  # so a potential of Redis command injection exists.
  def filtered_id
    params[:id].tr('^A-Za-z0-9.', '')
  end

  def index
    @queues = RedisQueue::Base.all
  end

  def destroy
    @queue = RedisQueue::Base.find filtered_id
    @queue.destroy
  end
end
```


### Messages

```ruby
class MessagesController
  def queue
    @queue ||= RedisQueue::Base.new('input')
  end

  def output_queue
    @output_queue ||= RedisQueue::Base.new('output')
  end

  def raw_message
    params[:message][:serialized]
  end

  def index
    @messages = queue.elements
    @messages = queue.backup_elements

    @messages = queue.pop_all
    @messages = queue.backed_up_pop_all
  end

  def update
    queue.insist do # Retry on optimistic lock fail
      queue.watch_backup # Optimistic locking

      if queue.raw_backup_elements.include? raw_message
        @message = queue.decode raw_message

        if @message.valid?

          queue.multi do
            queue.remove_raw_backup_element raw_message
            output_queue.push "some message"
          end

          respond_to do |format|
            format.html { render :nothing => true, status: :created }
          end
        else
          respond_to do |format|
            format.html { render partial: 'form', :locals => {message: @message}, status: :unprocessable_entity }
          end
        end
      else
        respond_to do |format|
          format.html { render text: "The message you are trying to update is gone", status: :gone }
        end
      end
    end
  end

  def destroy
    queue.remove_raw_element raw_message
  end
end
```


# Environment details

## Connection configuration

By default, Redis is expected to be found listening the 127.0.0.1:6379, but that can be configured in ``config/redis_call.yml``

```yml
development:
  host: localhost
  port: 6379

production:
  host: localhost
  port: 6379

test:
  host: localhost
  port: 6379
```


## Forks and Unicorn webserver

Forking the process require reconnection. For Unicorn webserver it may be done by stating in ``config/unicorn.rb``:

```ruby
before_fork do |server, worker|
  RedisCall.new.disconnect
end

after_fork do |server, worker|
  RedisCall.new.connect
end
```


## Copyright and License

```
Copyright 2011 Stanislav Senotrusov

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Contributing

Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.
