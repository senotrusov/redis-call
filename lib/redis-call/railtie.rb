
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


class RedisCall::Railtie < Rails::Railtie
  initializer :redis_call do |app|
    
    if File.exists?(config_file = Rails.root + 'config' + 'redis_call.yml')
      if (yaml = YAML.load_file(config_file)).kind_of?(Hash)
        if (env = yaml[Rails.env]).kind_of?(Hash)
          RedisCall.config = env.with_indifferent_access
        else
          STDERR.write "WARNING: #{config_file} hash does not contains key for current #{Rails.env} environment\n"
        end
      else
        STDERR.write "WARNING: #{config_file} does not contains a Hash\n"
      end
    end
    
  end
end

class RedisQueue::Railtie < Rails::Railtie
  config.to_prepare do

    if File.exists?(config_file = Rails.root + 'config' + 'redis_queue.yml')
      if (yaml = YAML.load_file(config_file)).kind_of?(Hash)
        RedisQueue.config = yaml.select {|queue, options| queue != "templates"}.with_indifferent_access
      else
        STDERR.write "WARNING: #{config_file} does not contains a Hash\n"
      end
    end

  end
end
