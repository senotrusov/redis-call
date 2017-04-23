
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


begin
  require 'jeweler'

  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "redis-call"
    gemspec.licenses = ['Apache-2.0']
    gemspec.summary = "Redis access library for Ruby: threads, handy key names, transactions and queues"
    gemspec.authors = ["Stanislav Senotrusov"]
    gemspec.email = "stan@senotrusov.com"
    gemspec.homepage = "https://github.com/senotrusov/redis-call"

    gemspec.add_dependency 'hiredis'
    gemspec.add_dependency 'yajl-ruby'
  end

  Jeweler::GemcutterTasks.new

rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end
